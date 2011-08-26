from ndb import model

class CachedTile(model.Model):
  tile = model.BlobProperty(required=True)
  rendered = model.DateTimeProperty(required=True)
  operation_cost = model.IntegerProperty(required=True)
  render_time = model.FloatProperty(required=True)
  level = model.IntegerProperty(required=True)

  _use_datastore = False
  _use_memcache = False

  @classmethod
  def key_for_tile(cls, type, level, x, y):
    return model.Key(cls, '%s/%s/%s/%s' % (type, level, x, y))
