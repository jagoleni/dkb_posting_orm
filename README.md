# dkb_posting_orm
Create a DB from your DKB banking export csv

## Installation

1. Create conda env: `conda env create -f environment.yml`
2. Activate env: `conda activate dkb`
3. Export your Buchungen from DKB banking and execute script: `python create_db.py 123456.csv --output my_postings`  
A sqlite file has been created in your current dir, load this with your database client of your choice.
