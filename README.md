# eia_data_backup
Backup of data from U.S. Energy Information Administration Open Data (https://www.eia.gov/opendata/)
# Setup Python Project

uv python install 3.13
uv init <NAME OF PROJECT>
uv venv --python 3.13

Then activate the environment: .venv\Scripts\activate

# add packages
uv add <PACKAGE NAME>	

uv sync

uv remove <PACKAGE NAME>


# Install dependencies from pyproject.toml
uv pip install -r pyproject.toml --all-extras