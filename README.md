# Star Connections

## Description
Star Connections is a project that utilizes ETL (Extract, Transform, Load) to scrape data from the IMDb free dataset. The data is then transformed and loaded into a PostgreSQL database. This database stores information to facilitate the discovery of the shortest path between two given actors. The project also includes an additional branch that stores the transformed data.

### Key Features
- ETL process to scrape, transform, and load IMDb data.
- Utilizes the Breadth-First Search (BFS) algorithm to find the shortest path between actors.
- Implements Kevin Bacon's Six Degrees of Separation concept.

## Requirements
- Python 3.10
- PostgreSQL 12 (or another compatible database)

## Getting Started

### Clone the Repository
```
git clone https://github.com/ShymiY3/Star-Connections.git
```
### Create a Virtual Environment (Windows)
```
python -m venv venv
```

### Activate Virtual Environment (Windows)
```
venv\Scripts\Activate
```

### Install Dependencies
```
pip install -r requirements.txt
```

### Create a Database
- Create a PostgreSQL database (in the project, it is named `star_connectionsdb`).

### Configure Environment Variables
- Create a `.env` file and fill in the following information:
  - `DB_ADDRESS`: Database address.
  - `DB_USERNAME`: Database username.
  - `DB_PASSWORD`: Database password.
  - `DJANGO_SECRET_KEY`: Django secret key.

### Optional: Generate Django Secret Key
You can generate a new Django secret key using the following command:
```
python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"
```
### Initialize the Database
```
python init_db.py
```
### Run the Parser
- Run the parser to transform and load the data into the database
- Use the -h or --help flag to display helpful information about the parser.
```
python database/parser.py
```
### Perform Migrations
```
python manage.py makemigrations
```
```
python manage.py migrate
```
## Run Server
```
python manage.py runserver
```