## why should i use pip install . or pip install -e .? and what is the difference? what is the output of these commands and how/when is the output utilized? what happens without it?

You should use pip install . or pip install -e . to make your package importable from anywhere in your Python environment, not just from the project root. Here’s what each does:

`pip install .`
### What it does:
  - Builds and installs your package into your Python environment (site-packages) as a regular, static install.
### Output:
  - Copies your package files into the environment’s site-packages directory.
### When to use:
  - When you want to use your package as a dependency, or run scripts from anywhere, and don’t plan to change the code frequently.

`pip install -e .` (editable/development mode)
### What it does:
- Installs your package as a “link” to your source directory. Changes you make to your code are immediately reflected without reinstalling.
### Output:
- Creates a special .egg-link file in site-packages pointing to your source directory.
### When to use:
- During development, so you can edit code and immediately test changes.
### What happens without it?
- Python won’t find your package unless you run scripts from the project root or set PYTHONPATH manually.
- Imports like import data_etl_app may fail if you run scripts from outside the root folder.
### Summary:
- Use pip install . for production or when you don’t plan to change code.
- Use pip install -e . for development.
- Both commands make your package importable everywhere in your environment.
- Without them, you may face import errors unless you manage paths manually.

# Other notes:
- `python3.12 -m venv .venv` if first time
- activate venv in shell each time: `source .venv/bin/activate`
- playwright install even after installing app in -e mode