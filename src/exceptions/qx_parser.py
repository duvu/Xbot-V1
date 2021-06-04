import argparse


class ArgumentParserError(Exception):
    pass


class QXParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)
