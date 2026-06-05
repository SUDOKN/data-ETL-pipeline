if a module has _app suffix, it has at least one of the following properties
- fastapi http server
- bots

Examples:
core for example is really the innermost foundation on which other apps are running. 
it contains various db models, services etc shared by multiple apps hence consolidated as core.

open_ai_key_app has a batch file processor bot.

data_etl_app has a fastapi server as well as queue bots.