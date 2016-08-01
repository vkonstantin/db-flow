import re
import shutil
import os


class FuncParam(object):
  name = ''
  ptype = ''
  def_value = ''

class FuncCall(object):
  sname = ''
  params = []
  def __init__(self):
    params = []

class CommonFunc(object):
  id = ''
  schema = ''
  name = ''
  params = ''
  params_in = []
  body = ''
  dump = ''
  def __init__(self):
    self.params_in = []

  def sname(self):
    return self.schema+'.'+self.name

  def cleanDump(self, dump):
    return dump.split('SET search_path')[0]

  def setSchemaNameToDump(self, otype, schema, dump):
    return '--\n'+dump.replace('CREATE '+otype+' ', 'CREATE '+otype+' '+schema+'.')


class AggFunc(CommonFunc):
  def __init__(self):
    super().__init__()

  def fillFromDump(self, dump):
    dump = self.cleanDump(dump)
    r = re.search(r'CREATE\sAGGREGATE\s(?P<name>[\w\d_]+)\((?P<params>[\w\W]+)\)\s+\((?P<body>[^;]+)\);', dump)

    if not r is None:
      self.dump = self.setSchemaNameToDump('AGGREGATE', self.schema, dump)
      self.name = r.group('name').strip()
      self.params = r.group('params').strip()
      self.params_in = self.params.split(',')
      self.body = r.group('body').strip()

  def save(self, folder):
    path = folder+'/'+self.schema + '/aggregates/'
    if not os.path.exists(path):
      os.makedirs(path)

    with open(path+self.id+'.sql',"w+") as f:
      f.write(self.dump)
      f.close()


class Func(CommonFunc):
  params_out = []
  returns = ''
  lang = ''
  delimeter = ''
  subfuncs = []
  depends_on = []

  def __init__(self):
    super().__init__()
    self.params_out = []
    self.subfuncs = []
    self.depends_on = []


  def parseBody(self):
    body = self.excludeComments(self.body)
    while True:
      r = re.search(r'(?P<before>[\w]+\s+)*(?P<fnc>[\w\d_]+\.[\w\d_]+)\s*\((?P<params>[\w\W]*)$', body)
      if r is None:
        break

      body = r.group('params')

      if (not r.group('before') is None) and r.group('before') != '' and r.group('before').strip().lower() == 'into':
        continue

      f = FuncCall()
      f.sname = r.group('fnc').strip()
      f.params = self.parseCallParams(body)
      self.subfuncs.append(f)

  def excludeComments(self, str):
    lines = str.split('\n')
    str = ''
    for line in lines:
      str = str + '\n' + (line.split('--')[0])

    #exclude strings
    s = ''
    is_in = False
    for i in str:
      if i == "'":
        if is_in == False:
          is_in = True
        else:
          is_in = False
      if is_in == False:
        s = s + i

    #exclude /* */
    str = ''
    is_in = False
    for i in range(len(s)-1):
      if s[i: i+2] == '/*':
        is_in = True
      if i>=2 and s[i-2:i] == '*/':
        is_in = False
      if is_in == False:
        str = str + s[i]
    return str

  def parseCallParams(self, str):
    params = []
    p = ''
    depth = 0

    for i in str:
      if i in ['(', '[']:
        depth = depth + 1
      if i in [')', ']']:
        depth = depth - 1

      if depth == -1:
        if len(p.strip())>0:
          params.append(p.strip())
        break

      if depth == 0 and i == ',':
        params.append(p.strip())
        p = ''
        continue
      p = p + i

    return params


  def parseParams(self):
    arr_params = self.params.strip().split(',')
    for param in arr_params:
      param_def = param.strip().split(' DEFAULT ')
      r = re.match(r'(?P<mode>((OUT)|(INOUT))*)\s*(?P<name>[\w\d_]+\s)*(?P<ptype>[\w\s\.]+)', param_def[0].strip())

      if not r is None:
        p = FuncParam()
        if not r.group('name') is None:
          p.name = r.group('name').strip()
        p.ptype = r.group('ptype').strip()
        if len(param_def) == 2:
          p.def_value = param_def[1].strip()

        mode = r.group('mode').strip() 
        if mode == 'OUT':
          self.params_out.append(p)
        elif mode == 'INOUT':
          self.params_out.append(p)
          self.params_in.append(p)
        else:
          self.params_in.append(p)


  def fillFromDump(self, dump):
    dump = self.cleanDump(dump)
    r = re.search(r'CREATE\sFUNCTION\s(?P<name>[\w\d\"_]+)\((?P<params>[\w\W]*)\)\sRETURNS\s(?P<returns>[\w\W]+)\sLANGUAGE\s(?P<lang>[\w]+)[\s\w\d]+AS\s(?P<delimeter>[\S]+)\s', dump)

    if not r is None:
      self.dump = self.setSchemaNameToDump('FUNCTION', self.schema, dump)
      self.name = r.group('name').replace('"', '').strip()
      self.params = r.group('params').strip()
      self.returns = r.group('returns').strip()
      self.lang = r.group('lang').strip()
      self.delimeter = r.group('delimeter').strip()
      self.body = dump.split(self.delimeter)[1]
      self.parseParams()
      self.parseBody()

  def save(self, folder):
    path = folder+'/'+self.schema + '/functions/'
    if not os.path.exists(path):
      os.makedirs(path)

    with open(path+self.id+'.sql',"w+") as f:
      f.write(self.dump)
      for df in self.depends_on:
        f.write("\n-- DependsOn: "+df)
      f.close()

class Dump(object):
  functions = []
  aggregates = []
  disable_schemas = []

  def __init__(self):
    self.functions = []
    self.aggregates = []
    self.disable_schemas = []

  def fillFromFile(self, filename):
    with open(filename, 'r') as content_file:
      content = content_file.read()

      dumps = content.split('-- Name: ')
      for dump in dumps:
        dump = '-- Name: '+dump
        r = re.match(r'--\sName:\s(?P<name>[\w\W]+);\sType:\s(?P<type>[\w]+);\sSchema:\s(?P<schema>[\w_]+);', dump)
        if not r is None:
          if r.group('schema') in self.disable_schemas:
            continue

          if r.group('type') == 'FUNCTION':
            f = Func()
            f.schema = r.group('schema')
            f.id = f.schema+'.'+r.group('name').strip()
            f.fillFromDump(dump)
            self.functions.append(f)
            #print("\n\n\n>>>", f.id)
          if r.group('type') == 'AGGREGATE':
            af = AggFunc()
            af.schema = r.group('schema')
            af.id = af.schema+'.'+r.group('name').strip()
            af.fillFromDump(dump)
            self.aggregates.append(af)
      self.fillFunctionDependensies()

  def printFunc(self, **kwords):
    for f in self.functions:
      if 'sname' in kwords:
        if f.sname() == kwords['sname']: 
          self._printFunc(f)


  def _printFunc(self, f):
    print('Func:', f.id)
    for p in f.params_in:
      print('Param:', p.name, '|', p.ptype, '|', p.def_value)

    for sf in f.subfuncs:
      print('Call func:', sf.sname, "params:", len(sf.params))
      for cp in sf.params:
        print('      ', cp)

  def save(self, folder):
    shutil.rmtree(folder, ignore_errors=True)
    os.makedirs(folder)
    for f in self.functions:
      f.save(folder)
    for ag in self.aggregates:
      ag.save(folder)

  def fillFunctionDependensies(self):
    for f in self.functions:
      if f.schema in self.disable_schemas:
        continue
      if f.lang in ['plpythonu', 'plpython']:
        continue

      for sf in f.subfuncs:
        found = False
        for f2 in self.functions:
          if f2.schema in self.disable_schemas:
            continue

          max_params = len(f2.params_in)
          min_params = 0
          for p2 in f2.params_in:
            if p2.def_value == '':
              min_params = min_params + 1

          if sf.sname == f2.sname() and len(sf.params) >= min_params and len(sf.params) <= max_params:
            found = True
            f.depends_on.append(f2.id)
            break

        for ag in self.aggregates:
          if sf.sname == ag.sname() and len(sf.params) == len(ag.params_in):
            found = True
            f.depends_on.append(ag.id)
            break

        if found == False:
          warning = "WARNING! Function {0}(with {1} params) not found!".format(sf.sname, len(sf.params))
          f.depends_on.append(warning)
          print(warning+' Called from function', f.id)
      f.depends_on = set(f.depends_on)





