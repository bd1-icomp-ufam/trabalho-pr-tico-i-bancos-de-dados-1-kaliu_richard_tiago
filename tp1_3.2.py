import psycopg2
from config import load_config
import re
import time

# Configurações
DATA_FILE = 'amazon-meta.txt'
BATCH_SIZE = 1000

# Compilando expressões regulares para parsing eficiente
product_id_pattern = re.compile(r'^Id:\s+(\d+)')
asin_pattern = re.compile(r'^ASIN:\s+(\S+)')
title_pattern = re.compile(r'^title:\s+(.+)')
group_pattern = re.compile(r'^group:\s+(.+)')
salesrank_pattern = re.compile(r'^salesrank:\s+(\d+)')
similar_pattern = re.compile(r'^similar:\s+(\d+)\s+(.+)')
category_pattern = re.compile(r'\|([^\[]+)\[(\d+)\]')
review_pattern = re.compile(r'^(\d{4}-\d{1,2}-\d{1,2})\s+cutomer:\s+(\S+)\s+rating:\s+(\d+)\s+votes:\s+(\d+)\s+helpful:\s+(\d+)')

def create_tables():
    commands = [
        
        
        """
        CREATE TABLE IF NOT EXISTS "product" (
            "asin" VARCHAR(10),
            "title" VARCHAR(2000),
            "group_name" VARCHAR(2000),
            "salesrank" INTEGER,
            PRIMARY KEY("asin")
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS "similar_product" (
            "product_asin" VARCHAR(10),
            "similar_asin" VARCHAR(10)
            
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS "category" (
            "category_id" INTEGER,
            "category_name" VARCHAR(500),
            PRIMARY KEY("category_id")
        
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS "product_category" (
            "product_asin" VARCHAR(10),
            "category_id" INTEGER 
        );
        
        """,
        """
        CREATE TABLE IF NOT EXISTS "product_review"(
            "product_asin" VARCHAR(10) ,
            "time" DATE ,
            "user_id" VARCHAR(100) ,
            "rating" INTEGER ,
            "total_votes" INTEGER ,
            "helpfulness_votes" INTEGER 
    
      );
        """

    ]
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                for command in commands:
                    cur.execute(command)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


def process_file_and_populate():
    config = load_config()
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    start_time = time.time()
    processed_products = 0

    with open(DATA_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        product = {}
        similar_asins = []
        categories = []
        reviews = []
        

        cursor.execute('BEGIN TRANSACTION')
        for line in f:
            
            
            line = line.strip()

            if not line:
                continue

            if line.startswith('Id: '):
                if product:
                    insert_product(cursor, product, categories, similar_asins, reviews)
                    processed_products += 1

                    if processed_products % BATCH_SIZE == 0:
                        conn.commit()
                        elapsed_time = time.time() - start_time
                        print(f"Processed {processed_products} products in {elapsed_time:.2f} seconds")
                        cursor.execute('BEGIN TRANSACTION')

                # Reset para o próximo produto
                product = {}
                categories = []
                reviews = []
                similar_asins = []

                match = product_id_pattern.match(line)
                if match:
                    product['id'] = int(match.group(1))
            elif line.startswith('ASIN:'):
                match = asin_pattern.match(line)
                if match:
                    product['asin'] = match.group(1)
            elif line.startswith('title:'):
                match = title_pattern.match(line)
                if match:
                    product['title'] = match.group(1)
            elif line.startswith('group:'):
                match = group_pattern.match(line)
                if match:
                    product['group_name'] = match.group(1)
            elif line.startswith('salesrank:'):
                match = salesrank_pattern.match(line)
                if match:
                    product['salesrank'] = int(match.group(1))
            elif line.startswith('similar:'):
                match = similar_pattern.match(line)
                if match:
                    similar_asins = match.group(2).split()
            elif line.startswith('|'):
                matches = category_pattern.findall(line)
                if matches:
                    for match in matches:
                        category_name = match[0].strip()  # O nome da categoria
                        category_id = match[1]   # O ID
                        

                        if (category_id, category_name) not in categories:
                            
                            categories.append((category_id, category_name))
            elif review_pattern.match(line):
                match = review_pattern.match(line)
                
                if match:
                    reviews.append(match.groups())

        # Inserir o último produto
        if product:
            
            insert_product(cursor, product, categories, similar_asins, reviews)
            processed_products += 1
        
        

        conn.commit()
        conn.close()

        total_time = time.time() - start_time
        print(f"Finished processing {processed_products} products in {total_time:.2f} seconds")


def insert_product(cursor, product, categories, similar_asins, reviews):
    
    cursor.execute('''
            INSERT INTO "product" (asin, title, group_name, salesrank)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT("asin")
            DO NOTHING;
        ''', (
        product.get('asin'),
        product.get('title'),
        product.get('group_name'),
        product.get('salesrank', 0)
    ))
    #product_asin = cursor.fetchone()[1]

    for asin in similar_asins:
        cursor.execute('''
            INSERT INTO "similar_product" ("product_asin", "similar_asin")
            VALUES (%s, %s);
        ''', (product.get('asin'), asin))

    for category_id, category_name in categories:
        cursor.execute('''
            INSERT INTO "category" ("category_id", "category_name")
            VALUES (%s, %s)
            ON CONFLICT("category_id")
            DO NOTHING;
        ''', (category_id, category_name))

    for category_id, _ in categories:
        cursor.execute('''
            INSERT INTO "product_category" ("product_asin", "category_id")
            VALUES (%s, %s);
        ''', (product.get('asin'), category_id))

    for review in reviews:
        cursor.execute('''
            INSERT INTO "product_review" (product_asin, time, user_id, rating, total_votes, helpfulness_votes)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (product.get('asin'), review[0], review[1], review[2], review[3], review[4]))
    

def restriction():
    config = load_config()
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    cursor.execute('''
        ALTER TABLE "product"
        ALTER COLUMN "asin" SET NOT NULL;
    ''')
    
    cursor.execute('''
        ALTER TABLE "product"
        ALTER COLUMN "salesrank" SET NOT NULL;
    ''')

    cursor.execute('''
        ALTER TABLE "category"
        ALTER COLUMN "category_id" SET NOT NULL;
    ''')

    cursor.execute('''
         ALTER TABLE "category"
         ALTER COLUMN "category_name" SET NOT NULL;
     ''')
    
    cursor.execute('''
         ALTER TABLE "product_review"
         ALTER COLUMN "time" SET NOT NULL;
     ''')

    cursor.execute('''
        ALTER TABLE "product_review"
        ALTER COLUMN "user_id" SET NOT NULL;
    ''')

    cursor.execute('''
        ALTER TABLE "product_review"
        ALTER COLUMN "rating" SET NOT NULL;
    ''')

    cursor.execute('''
        ALTER TABLE "product_review"
        ALTER COLUMN "total_votes" SET NOT NULL;
    ''')

    cursor.execute('''
        ALTER TABLE "product_review"
        ALTER COLUMN "helpfulness_votes" SET NOT NULL;
    ''')

    cursor.execute('''
        ALTER TABLE "similar_product"
        ADD CONSTRAINT "fk_similar_prod" FOREIGN KEY ("product_asin") REFERENCES "product" ("asin");
    ''')

    cursor.execute('''
        ALTER TABLE "product_category"
        ADD CONSTRAINT "fk_categ_prod" FOREIGN KEY ("product_asin") REFERENCES "product" ("asin");
    ''')

    cursor.execute('''
        ALTER TABLE "product_category"
        ADD CONSTRAINT "fk_categ_id" FOREIGN KEY ("category_id") REFERENCES "category" ("category_id");
    ''')

    cursor.execute('''
        ALTER TABLE "product_review"
        ADD CONSTRAINT "fk_review_asin" FOREIGN KEY ("product_asin") REFERENCES "product" ("asin");
    ''')    
    
    

    



    

    

    conn.commit()
    conn.close()


if __name__ == '__main__':
    create_tables()
    process_file_and_populate()
    restriction()
    




