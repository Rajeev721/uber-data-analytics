from configparser import ConfigParser

def dbconnection(DB_NAME):
    db_config = {}
    cp = ConfigParser()
    # param = cp.read_file(open('config'))
    cp.read('config')
    if cp.has_section(DB_NAME):
        param = cp.items(DB_NAME)
        for par in param:
            db_config[par[0]] = par[1]
    else:
        pass
    return db_config