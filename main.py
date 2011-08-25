import os
import urlparse
import webapp2
from google.appengine.api import backends
from google.appengine.api import urlfetch
from webapp2_extras import jinja2

import mandelbrot

class BaseHandler(webapp2.RequestHandler):
  @webapp2.cached_property
  def jinja2(self):
    return jinja2.get_jinja2(app=self.app)

  def render_template(self, filename, **template_args):
    body = self.jinja2.render_template(filename, **template_args)
    self.response.write(body)


class IndexHandler(BaseHandler):
  def get(self):
    self.render_template('index.html')


class TileHandler(BaseHandler):
  def get(self, level, x, y):
    url = urlparse.urljoin(backends.get_url('renderer'),
                           '/backend/render_tile/%s/%s/%s' % (level, x, y))
    tile = urlfetch.fetch(url)
    self.response.headers['Content-Type'] = 'image/png'
    self.response.write(tile.content)


class BackendTileHandler(BaseHandler):
  def get(self, level, x, y):
    image = mandelbrot.render_tile(int(level), int(x), int(y))
    self.response.headers['Content-Type'] = 'image/png'
    image.save(self.response.out, 'PNG')


application = webapp2.WSGIApplication([
    ('/', IndexHandler),
    ('/exabrot_files/(\d+)/(\d+)_(\d+).png', TileHandler),
    ('/backend/render_tile/(\d+)/(\d+)/(\d+)', BackendTileHandler),
], debug=True)