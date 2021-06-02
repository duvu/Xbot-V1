import argparse


class ArgumentParserError(Exception):
    pass


class MyXParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)
