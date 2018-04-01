

def convert(data):
  ''' convert bytes to string in py3'''
  if isinstance(data, bytes):      return data.decode()
  if isinstance(data, (str, int)): return str(data)
  if isinstance(data, dict):       return dict(list(map(convert, list(data.items()))))
  if isinstance(data, tuple):      return tuple(map(convert, data))
  if isinstance(data, list):       return list(map(convert, data))
  if isinstance(data, set):        return set(map(convert, data))
  return data

