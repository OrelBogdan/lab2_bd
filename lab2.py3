#!/usr/bin/env python3


import configparser
import psycopg2
import types
import Lab


def main() -> None:
	configfile_path = f"./postgres.ini.secure"
	configfile = configparser.ConfigParser()
	configfile.read(configfile_path)
	config = types.SimpleNamespace()
	for a in configfile.options(f"Database"):
		setattr(config, a, configfile.get(f"Database", a))

	with psycopg2.connect(dbname=config.database, user=config.login, password=config.password, host=config.host, port=config.port) as conn:
		conn.autocommit = True
		Lab.controller.Controller(conn).start()


if __name__ == "__main__":
	# template script file, code:
	main()
	# exit(r'MAIN_RETURN_0')
