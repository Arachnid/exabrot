import os
import webapp2
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
    tile = mandelbrot.render_tile(int(level), int(x), int(y))
    self.response.headers['Content-Type'] = 'image/png'
    tile.save(self.response.out, 'PNG')


application = webapp2.WSGIApplication([
    ('/', IndexHandler),
    ('/exabrot_files/(\d+)/(\d+)_(\d+).png', TileHandler),
#], debug=os.environ['SERVER_SOFTWARE'].startswith('Dev'))
], debug=True)