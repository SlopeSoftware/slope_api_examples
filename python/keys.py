import configparser

config = configparser.ConfigParser()
config.read("config.ini")

api_key: str = config["API"]["Key"]
api_secret: str = config["API"]["Secret"]