#!/usr/bin/env python
import Lab.view
import Lab.model
import Lab.utils

__all__ = ["Controller"]


class Controller(object):
	def __init__(self, dbconn, model=Lab.model, view=Lab.view):
		self._dbconn = dbconn
		self._model = model
		self._view = view.View(self)
		self._schema = Lab.model.Shop(self._dbconn)

	@property
	def schema(self):
		return self._schema

	def start(self) -> None:
		self._view.mainMenu()

	@property
	def __lab_console_interface__(self):
		MVC = 1
		if not MVC:
			result = Lab.utils.lab_console_interface(self.schema)
			# result |= {f"exit": Lab.utils.menuReturn(f"User exit"), }
		else:
			result = Lab.utils.LabConsoleInterface({
				**{f'"{table.table}" table': (lambda table: lambda: Lab.utils.LabConsoleInterface({
					f"describe": table.describe,
					f"show data": table.showData,
					f"add data": table.addData,
					f"edit data": table.editData,
					f"remove data": table.removeData,
					f"random fill": table.randomFill,
					f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
				}, promt=table.promt))(table) for table in self.schema},
				f'Schema "{self.schema}" utils': lambda: Lab.utils.LabConsoleInterface({
					f"reinit": self.schema.reinit,
					f"random fill": self.schema.randomFill,
					f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
				}, promt=f'Schema "{self.schema}" utils'),
				f'Dynamic search': lambda: Lab.utils.LabConsoleInterface({
					**{dynamicsearch.name: (lambda dynamicsearch: lambda: Lab.utils.LabConsoleInterfaceDynamicUpdate(lambda: Lab.utils.LabConsoleInterface({
						**{search_name: (lambda search_name, search: lambda: Lab.utils.LabConsoleInterfaceDynamicUpdate(lambda: Lab.utils.LabConsoleInterface({
							**{f"Property {property_id} {property_instance}": (lambda property_id, property_instance: lambda: Lab.utils.LabConsoleInterfaceDynamicUpdate(lambda: Lab.utils.LabConsoleInterface({
								f"ignore": property_instance.reset,
								f"<": property_instance._lt,
								f"<=": property_instance._le,
								f"=": property_instance._eq,
								f"!=": property_instance._ne,
								f">=": property_instance._ge,
								f">": property_instance._gt,
								f"LIKE": property_instance._like,
								f"set NULL": property_instance.setNull,
								f"set constant": property_instance.setConstant,
								f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
							}, promt=property_instance.promt)))(property_id, property_instance) for property_id, property_instance in enumerate(search.search_criterias.append(), 1)},
							f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
						}, promt=search.promt)))(search_name, search) for search_name, search in dynamicsearch.search.items()},
						f"execute": dynamicsearch.execute,
						f"sql": lambda: print(dynamicsearch.sql),
						f"reset": dynamicsearch.reset,
						f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
					}, promt=dynamicsearch.promt)))(dynamicsearch) for dynamicsearch in self.schema.dynamicsearch.values()},
					f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
				}, promt=f"""Schema "{self.schema}" dynamic search interface"""),
			}, promt=f'MVC schema "{self.schema}" interface')
		return result


# f'"Authors" table': lambda: Lab.utils.LabConsoleInterface({
# 	f"describe": self.schema.tables.Authors.describe,
# 	f"show data": self.schema.tables.Authors.showData,
# 	f"add data": self.schema.tables.Authors.addData,
# 	f"edit data": self.schema.tables.Authors.editData,
# 	f"remove data": self.schema.tables.Authors.removeData,
# 	f"random fill": self.schema.tables.Authors.randomFill,
# 	f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
# }, promt=self.schema[f"Authors"].promt),

# f"Books": lambda: Lab.utils.LabConsoleInterface({
# 	# f"Author": lambda: self.schema.dynamicsearch[0].search[f"Author"],
# 	f"Author": lambda: Lab.utils.LabConsoleInterfaceDynamicUpdate(lambda: Lab.utils.LabConsoleInterface({
# 		**{f"Property {a} {b}": (lambda x: lambda: x)(Lab.utils.LabConsoleInterfaceDynamicUpdate(lambda: Lab.utils.LabConsoleInterface({
# 			f"ignore": b.reset,
# 			f"<": b._lt,
# 			f"<=": b._le,
# 			f"=": b._eq,
# 			f"!=": b._ne,
# 			f">=": b._ge,
# 			f">": b._gt,
# 			f"LIKE": b._like,
# 			f"set NULL": b.setNull,
# 			f"set constant": b.setConstant,
# 			f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
# 		}, promt=b.promt))) for a, b in enumerate(self.schema.dynamicsearch[f"Books"].search[f"Author"].search_criterias.append(), 1)},
# 		f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
# 	}, promt=self.schema.dynamicsearch[f"Books"].search[f"Author"].promt)),
# 	f"execute": self.schema.dynamicsearch[f"Books"].execute,
# 	f"sql": lambda: print(self.schema.dynamicsearch[f"Books"].sql),
# 	f"reset": self.schema.dynamicsearch[f"Books"].reset,
# 	f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
# }, promt=self.schema.dynamicsearch[f"Books"].promt),

# lambda: Lab.utils.LabConsoleInterface({
# f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
# }, promt=f""),

def _test() -> None:
	pass


if __name__ == "__main__":
	_test()
