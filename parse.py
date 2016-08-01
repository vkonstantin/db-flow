from dump_parser import *

proxy = Dump()
proxy.disable_schemas = ['public', 'pgtap', 'londiste', 'pgq', 'pgq_ext', 'pgq_node']
#proxy.fillFromFile('hv_proxy.sql')
#proxy.printFunc(sname = 'billing.balance_change')
#proxy.save('hv_proxy')

#proxy.fillFromFile('test.sql')


part = Dump()
part.disable_schemas = ['public', 'pgtap', 'londiste', 'pgq', 'pgq_ext', 'pgq_node']
part.fillFromFile('hv_part01.sql')
part.save('hv_part')
#part.printFunc(sname='billing.note_ref_pay')
