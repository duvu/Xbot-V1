import os
from dotenv import load_dotenv

load_dotenv()
PYTHON_ENVIRONMENT = os.getenv('PYTHON_ENVIRONMENT')


async def helpX(ctx, *args, **kwargs):
    full_path = os.path.realpath(__file__)
    dir_path = os.path.dirname(full_path)
    help_file = os.path.join(dir_path, "help.vi.md")
    msg = ''
    if not os.path.exists(help_file):
        msg = "Help files not found."
    else:
        with open(help_file, "rt") as f:
            msg = f.read()

    if PYTHON_ENVIRONMENT == 'production':
        await ctx.send('%s' % msg, delete_after=300.0)
        await ctx.message.delete()
    else:
        print(msg)
