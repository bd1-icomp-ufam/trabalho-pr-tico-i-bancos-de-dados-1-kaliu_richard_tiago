import psycopg2
from rich.console import Console
from rich.table import Table
import pandas as pd
from config import load_config



# Função para executar a primeira consulta
def query1(asin):
    query = f"""
    (
        SELECT *
        FROM "product_review"
        WHERE "product_asin" = '{asin}'
        ORDER BY "helpfulness_votes" DESC, "rating" DESC
        LIMIT 5
    )
    UNION
    (
        SELECT *
        FROM "product_review"
        WHERE "product_asin" = '{asin}'
        ORDER BY "helpfulness_votes" DESC, "rating" ASC
        LIMIT 5
    );
    """
    return executar_consulta(query)

# Função para executar a segunda consulta
def query2(asin):
    query = f"""
    SELECT p2.*
    FROM "product" p1
    JOIN "similar_product" sp ON p1.asin = sp.product_asin
    JOIN "product" p2 ON sp.similar_asin = p2.asin
    WHERE p1.asin = '{asin}'  
    AND p2.salesrank < p1.salesrank;
    """
    return executar_consulta(query)

def query3(asin):
    query = f"""
        SELECT time, ROUND(AVG(rating),2) AS average_rating
        FROM product_review
        WHERE product_asin = '{asin}'
        GROUP BY time
        ORDER BY time;

    """
    return executar_consulta(query)

def query4():
    query = f"""
        WITH ranked_products AS (
        SELECT 
            p.asin,
            p.title,
            p.group_name,
            p.salesrank,
            ROW_NUMBER() OVER (PARTITION BY p.group_name ORDER BY p.salesrank) AS rank
        FROM product p
        WHERE p.salesrank > 0
    )
        SELECT asin, title, group_name, salesrank
        FROM ranked_products
        WHERE rank <= 10
        ORDER BY group_name, rank;
    
    """

    return executar_consulta(query)

def query5():
    query = """
        SELECT 
            product.group_name,
            ROUND(AVG(product_review.helpfulness_votes),2) AS avg_helpful_votes
        FROM 
            product_review
        JOIN 
            product ON product_review.product_asin = product.asin
        WHERE 
            product_review.rating >= 4
        GROUP BY 
            product.group_name
        ORDER BY 
            avg_helpful_votes DESC
        LIMIT 10;
    """
    return executar_consulta(query)

def query6():
    query = """
        SELECT 
            category.category_name,
            ROUND(AVG(product_review.helpfulness_votes),2) AS avg_helpful_votes
        FROM 
            product_review
        JOIN 
            product_category ON product_review.product_asin = product_category.product_asin
        JOIN 
            category ON product_category.category_id = category.category_id
        WHERE 
            product_review.rating >= 4  
        GROUP BY 
            category.category_name
        ORDER BY 
            avg_helpful_votes DESC
        LIMIT 5;
    
    """
    return executar_consulta(query)

def query7():
    query = f"""
        WITH ranked_reviews AS (
        SELECT 
            p.group_name,
            pr.user_id,
            COUNT(pr.user_id) AS review_count,
            ROW_NUMBER() OVER (PARTITION BY p.group_name ORDER BY COUNT(pr.user_id) DESC) AS rank
        FROM 
            product_review pr
        JOIN 
            product p ON pr.product_asin = p.asin
        GROUP BY 
            p.group_name, pr.user_id
        )
        SELECT 
            group_name, 
            user_id, 
            review_count
        FROM 
            ranked_reviews
        WHERE 
            rank <= 10
        ORDER BY 
            group_name, rank;


    """
    return executar_consulta(query)

# Função para executar uma consulta e retornar resultados
def executar_consulta(query):
    config = load_config()
    conn = psycopg2.connect(**config)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Função para exibir resultados em uma tabela
def exibir_tabela(titulo, df):
    console = Console()
    table = Table(title=titulo)

    for col in df.columns:
        table.add_column(col)

    for index, row in df.iterrows():
        table.add_row(*[str(value) for value in row])

    console.print(table)

# Main
if __name__ == "__main__":
    
    
    while True:
        print("Escolha uma consulta para executar:")
        print("1: Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação")
        print("2: Dado um produto, listar os produtos similares com maiores vendas do que ele")
        print("3: Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada")
        print("4: Listar os 10 produtos líderes de venda em cada grupo de produtos")
        print("5: Listar os 10 produtos com a maior média de avaliações úteis positivas por produto")
        print("6: Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto")
        print("7: Listar os 10 clientes que mais fizeram comentários por grupo de produto")
        print("0: Sair")

        escolha = input("Digite o número da opção desejada: ")
        
        if escolha == '1':
            asin = input("Digite o ASIN do produto: ")
            df_reviews = query1(asin)
            exibir_tabela("", df_reviews)

        elif escolha == '2':
            asin = input("Digite o ASIN do produto: ")
            df_similares = query2(asin)
            exibir_tabela("", df_similares)

        elif escolha == '3':
            asin = input("Digite o ASIN do produto: ")
            df_prod_review = query3(asin)
            exibir_tabela("", df_prod_review)

        elif escolha == '4':
            df_sales = query4()
            exibir_tabela("", df_sales)

        elif escolha == '5':
            df_maior_media = query5()
            exibir_tabela("", df_maior_media)

        elif escolha == '6':
            df_maior_media_categoria = query6()
            exibir_tabela("", df_maior_media_categoria)

        elif escolha == '7': 
            df_mais_comentarios = query7()
            exibir_tabela("", df_mais_comentarios)            
        

        elif escolha == '0':
            print("Saindo...")
            break

        else:
            print("Opção inválida. Tente novamente.")
