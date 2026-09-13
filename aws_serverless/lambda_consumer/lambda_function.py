import json
import os
import uuid
import boto3
import pymysql

s3_client = boto3.client('s3')

DEST_BUCKET = os.environ['DEST_BUCKET']
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']

def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        port=3306,
        connect_timeout=5
    )

def lambda_handler(event, context):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            for record in event.get('Records', []):
                payload = json.loads(record['body'])
                
                # Busca a chave de CNPJ da mensagem recebida
                cnpj = payload.get('identificador_cnpj_empresa') or payload.get('CNPJ')
                
                if cnpj:
                    cnpj_clean = str(cnpj).zfill(14)
                    # Consulta na tabela gerada no Job 4 do Glue
                    sql = """
                        SELECT nome_completo_empresa, nome_segmento_empresa, percentual_indice 
                        FROM tb_delivery 
                        WHERE identificador_cnpj_empresa = %s
                    """
                    cursor.execute(sql, (cnpj_clean,))
                    result = cursor.fetchone()
                    
                    if result:
                        payload['nome_completo_empresa'] = result[0]
                        payload['nome_segmento_empresa'] = result[1]
                        payload['percentual_indice'] = float(result[2]) if result[2] is not None else 0.0
                        payload['status_enriquecimento'] = 'ENCONTRADO'
                    else:
                        payload['status_enriquecimento'] = 'NAO_ENCONTRADO'
                
                # Salva a mensagem tratada e enriquecida no bucket de destino
                file_key = f"enriquecidos/resultado_{uuid.uuid4().hex[:8]}.json"
                s3_client.put_object(
                    Bucket=DEST_BUCKET,
                    Key=file_key,
                    Body=json.dumps(payload, ensure_ascii=False),
                    ContentType='application/json'
                )
    finally:
        conn.close()
        
    return {'statusCode': 200, 'body': 'Processamento e enriquecimento concluídos'}