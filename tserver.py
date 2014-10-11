#!/usr/bin/env python
import pdb
import os
import json
import argparse
import logging
from logging.handlers import RotatingFileHandler
from pandas import Series, DataFrame
from redis import Redis
from tornado.ioloop import IOLoop
from tornado.httpserver import HTTPServer
from tornado.web import Application, RequestHandler, StaticFileHandler, FallbackHandler, HTTPError
from tornado.wsgi import WSGIContainer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, query
from sqlalchemy.ext.automap import automap_base, AutomapBase
import url

class ArgsParse(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        argparse.ArgumentParser.__init__(self, *args, **kwargs)
        self.add_argument('-u', '--uri', help='sqlalchemy connection uri', default='sqlite:///database.sqlite')
        self.add_argument('-o', '--host', help='server host', default='localhost')
        self.add_argument('-p', '--port', help='server port', default=8080)
        self.add_argument('-s', '--static', help='static folder path', default='static')
        self.add_argument('-t', '--templates', help='templates folder path', default='templates')
        self.add_argument('-l', '--log', help='logger full path', default=None)
        self.add_argument('-v', '--level', help='log level', default='INFO')
        self.add_argument('--certfile', help='ssl cert file location', default=os.path.join(os.path.dirname(os.path.realpath(__file__)),'server.csr'))
        self.add_argument('--keyfile', help='ssl key file location', default=os.path.join(os.path.dirname(os.path.realpath(__file__)),'server.key'))

class Server(Application):
    """tornado server that is run and uses command line arguments as configuration.
    logs to stream and has optional file handler with rotating 2gb.
    default port is 8080"""
    Base = automap_base()
    def __init__(self, **settings):
        """setting gets passed into tornado.web.Applicatbon"""
        self.logger = logging.getLogger('server')
        self.cache = Redis()
        self.engine = None
        self.Session = None
        self.templates = settings.get('templates', 'templates')
        self.static = settings.get('static', 'static')
        self.parse_args()
        self.handlers = url.handlers
        self.handlers.append(('r/static', StaticFileHandler, dict(path=self.static)))
        Application.__init__(self, self.handlers, **settings)

    def parse_args(self, **kwargs):
        """setup server via command line input arguments"""
        args = ArgsParse(**kwargs).parse_args()
        if self.templates != args.templates:
            self.templates = args.templates
        if self.static != args.static:
            self.static = args.static
        self.setup_logger(args)
        self.engine = create_engine(args.uri)
        self.Base.prepare(self.engine, reflect=True)
        self.Session = sessionmaker(bind=self.engine)
        self.args = args

    def ssl_config(self):
        self.logger.info('signing with {} {}'.format(self.args.certfile,self.args.keyfile))
        ssl_options = dict(certfile=self.args.certfile,keyfile=self.args.keyfile)
        server = HTTPServer(self,ssl_options=ssl_options)
        server.listen(self.args.port, address=self.args.host)
        return server

    def setup_logger(self, args):
        """logger setup with streamhandler and optional rotating file handler"""
        level = getattr(logging, args.level.upper())
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        self.logger.setLevel(level)

        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        sh.setLevel(level)
        self.logger.addHandler(sh)

        if args.log:
            fh = RotatingFileHandler(args.log, 'a', 2*1024**2, 2)
            fh.setFormatter(formatter)
            fh.setLevel(level)
            self.logger.addHandler(fh)

    def flask_configure(self, apps):
        """configure path and url for flask application"""
        if not isinstance(apps, list):
            apps = [apps]
        for app in apps:
            app.static_folder = self.static
            app.template_folder = self.templates
            wsgiapp = WSGIContainer(app)
            self.handlers.append(('/{}/.*'.format(app.__name__), 
                                  FallbackHandler, 
                                  dict(fallback=wsgiapp)))

class Query(query.Query):
    def __init__(self, table, session):
        query.Query.__init__(self, table, session)
        self.table = table

    def __getattr__(self, attr):
        if attr == 'query':
            return self.session.query(self.table)
        return object.__getattribute__(self, attr)

class ORMBase(AutomapBase):
    @property
    def pkey(self):
        if hasattr(self, '__table__'):
            return (i.name for i in self.__table__.columns if i.primary_key)
        raise AttributeError('instance does not have proper table binding')

    @property
    def pval(self):
        return tuple(map(str, (getattr(self, i) for i in self.pkey)))

    def __repr__(self):
        return ','.join(self.pval)

    def __eq__(self, other):
        return self.pval == other.pval

    def __ne__(self, other):
        return self.pval != other.pval

    def to_dict(self):
        return {k:v for k, v in vars(self).items() if not k.startswith('_')}

    def to_series(self):
        return Series(self.to_dict(), name=self.pval)

    @classmethod
    def query(cls, session):
        return Query(cls, session)

    @classmethod
    def to_json(cls, session, **kwargs):
        query = cls.query(session).filter_by(**kwargs)
        return json.dumps([i.to_dict() for i in query])

    @classmethod
    def to_df(cls, session, **kwargs):
        query = cls.query(session).filter_by(**kwargs)
        return DataFrame([i.to_series() for i in query])
        

class DBAPI(RequestHandler):
    def parse_query(self):
        table = self.get_argument('table')
        kwargs = self.get_arguments('kwargs')
        orm = self.application.Base.classes.get(table)
        return orm, kwargs

    def get(self):
        orm, kwargs = self.parse_query()
        if orm.__tablename__ in self.application.cache.keys():
            js = self.application.cache.get(orm.__tablename__)
        else:
            orm_sess = self.application.Session()
            objs = orm_sess.query(orm).filter_by(**kwargs).all() if kwargs else orm_sess.query(orm).all()
            objs = [ORMBase.to_dict(i) for i in objs]
            js = json.dumps(objs)
            self.application.cache.set(orm.__tablename__,js,ex=60*60)
        self.write(js)
        orm_sess.close()
        self.finish()

    def post(self):
        orm, kwargs = self.parse_query()
        sess = self.application.Session()
        objs = [orm(**kw) for kw in kwargs]
        sess.add_all(objs)
        try:
            sess.commit()
            self.write('success')
        except Exception as e:
            sess.rollback()
            sess.close()
            self.application.logger.error(str(e))
            raise HTTPError(404, 'failed to update')
        finally:
            sess.close()
            self.finish()

class Main(RequestHandler):
    def get(self):
        self.write('get')
        self.finish()

    def post(self):
        self.write('post')
        self.finish()

class Login(RequestHandler):
    def post(self):
        form = self.get_arguments('form')
        self.finish()

if __name__ == '__main__':
    app = Server()
    app.listen(app.args.port,address=app.args.host)
    IOLoop.configure('tornado.platform.asyncio.AsyncIOLoop')
    loop = IOLoop.instance().start()
