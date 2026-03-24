# Databricks notebook source
# CONEXÃO E LEITURA DOS DADOS BRUTOS EM TXT
# 1. Credenciais 
storage_account_name = "datalakeivangui" 
storage_account_key = "uptABhZb3+kqJP0ac8jkdKnyKsjJkmKEfE9XjK0poagNWb85gzkwQYl7T+TXByh8ZYpPFa9dgPyJ+ASt0G1gQA=="         
container_name = "bronze-dados-brutos"

# 2. Caminhos para as duas pastas específicas
path_dueof = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/dueof/*.txt"
path_liquidacao = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/liquidacao/*.txt"

# 3. Leitura dos Empenhos (dueof)
df_dueof_bruto = spark.read \
    .option(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key) \
    .text(path_dueof)

# 4. Leitura das Liquidações
df_liq_bruto = spark.read \
    .option(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key) \
    .text(path_liquidacao)

# Testando se os dois funcionam
print("Amostra de Empenhos (dueof):")
display(df_dueof_bruto.limit(5))

print("Amostra de Liquidações:")
display(df_liq_bruto.limit(5))

# COMMAND ----------

# --- OTIMIZAÇÃO: MEMORY CACHE (SEM GRAVAÇÃO EM DISCO) ---

print("Carregando e fixando dados na memória do cluster...")

# 1. O comando .cache() avisa ao Spark: "Leia o TXT uma vez e guarde na RAM"
df_dueof_pronto = df_dueof_bruto.cache()
df_liq_pronto = df_liq_bruto.cache()

# 2. O comando .count() é uma "Ação" que força o Spark a ler os dados AGORA
# Sem isso, o cache só aconteceria na próxima vez que fosse dado um display
print(f"DUEOF carregado: {df_dueof_pronto.count()} linhas.")
print(f"Liquidações carregadas: {df_liq_pronto.count()} linhas.")

print("Sucesso! Os dados estão presos na memória e prontos para o fatiamento.")

# COMMAND ----------

#IMPORTACOES
from pyspark.sql.functions import col, trim, lit, concat, sum, when, substring, to_date, lpad, min
from pyspark.sql.types import DecimalType
from pyspark.sql import Window

# COMMAND ----------

#FATIA A TRIPA E CRIA UM DATAFRAME COM TODOS DADOS DISPONIVEIS
df_dueof_estruturado = df_dueof_pronto.select(
    col("value").substr(1, 2).alias("TIPO_DUEOF"),
    col("value").substr(3, 23).alias("CHAVE"),
    col("value").substr(3, 16).alias("NUMEMPENHO"),
    col("value").substr(26, 8).alias("DATA"),
    col("value").substr(34, 15).alias("NUMPROCESSO"),
    (col("value").substr(49, 14).cast(DecimalType(18, 2)) / 100).alias("VALOR"),
    col("value").substr(63, 14).alias("NUMR_CPF_CNPJ"),
    col("value").substr(77, 5).alias("FORMALIDADE_FINALIDADE"),
    col("value").substr(82, 11).alias("CONTA_BANCO_DEBITO"),
    col("value").substr(93, 11).alias("CONTA_BANCO_CREDITO"),
    col("value").substr(104, 2).alias("GRUPO_DESPESA"),
    col("value").substr(106, 60).alias("NOME_CREDOR"),
    col("value").substr(166, 9).alias("CODIGO_BANCO_DEBITO"),
    col("value").substr(175, 9).alias("CODIGO_BANCO_CREDITO"),
    col("value").substr(184, 3).alias("PRAZO_APLICACAO"),
    col("value").substr(187, 2).alias("STATUS_DOCUMENTO"),
    col("value").substr(189, 8).alias("DATA_DO_STATUS"),
    col("value").substr(197, 8).alias("NATUREZA"),
    col("value").substr(205, 13).alias("CODIGO_DO_PATRIMONIO"),
    col("value").substr(218, 1).alias("TIPO_EMPENHO"),
    col("value").substr(219, 10).alias("NUMERO_DECRETO"),
    col("value").substr(229, 8).alias("DATA_LEI"),
    col("value").substr(237, 7).alias("NUMERO_LEI"),
    col("value").substr(244, 2).alias("FUNCAO"),
    col("value").substr(246, 3).alias("SUB_FUNCAO"),
    col("value").substr(249, 4).alias("PROGRAMA"),
    col("value").substr(253, 4).alias("ACAO"),
    col("value").substr(257, 8).alias("FONTE"),
    col("value").substr(265, 12).alias("RECEITA_DEBITO"),
    col("value").substr(277, 12).alias("RECEITA_CREDITO"),
    col("value").substr(289, 13).alias("NUMERO_PDF"),
    col("value").substr(302, 2).alias("MODALIDADE_APLICACAO"),
    col("value").substr(304, 11).alias("FILLER")
)
# Visualização do resultado final
display(df_dueof_estruturado.limit(13))

# COMMAND ----------

#FATIA A TRIPA E CRIA DOIS DATAFRAMES - TXT LIQ TEM 2 LAYOUTS
df_raw_D = df_liq_pronto.filter(col("value").substr(1, 1) == "D")
df_raw_M = df_liq_pronto.filter(col("value").substr(1, 1) == "M")

# ESTRUTURAÇÃO DO REGISTRO 'D' 
df_liq_D = df_raw_D.select(
    col("value").substr(1, 1).alias("TIPO_DUEOF"), 
    col("value").substr(2, 16).alias("NUMEMPENHO"),
    (col("value").substr(18, 17).cast(DecimalType(19, 2)) / 100).alias("VALOR"),
    col("value").substr(35, 8).alias("DATA"),
    col("value").substr(43, 2).alias("TIPODOCUMENTO"),
    col("value").substr(45, 8).alias("DATADOCUMENTO"),
    col("value").substr(53, 20).alias("NUMERODOCUMENTO"),
    trim(col("value").substr(73, 120)).alias("DESCRICAO"),
    col("value").substr(193, 9).alias("NUMEROSERIE"),
    col("value").substr(202, 5).alias("MODELONOTA"),
    col("value").substr(207, 8).alias("DATAVALIDADENOTA"),
    col("value").substr(215, 8).alias("DATA DE REFENCIA"),
    col("value").substr(223, 20).alias("NUMERO_FILA"),
    trim(col("value").substr(243, 150)).alias("DESC_FILA")
)

# ESTRUTURAÇÃO DO REGISTRO 'M'
df_liq_M = df_raw_M.select(
    col("value").substr(1, 1).alias("TIPO_DUEOF"),
    col("value").substr(2, 16).alias("NUMEMPENHO"),
    col("value").substr(18, 20).alias("NUMERODOCUMENTO"),
    col("value").substr(38, 4).alias("SEQMOVLIQ"),
    col("value").substr(42, 1).alias("TIPOMOVIMENTO"),
    (col("value").substr(43, 17).cast(DecimalType(19, 2)) / 100).alias("VALOR"),
    col("value").substr(60, 8).alias("DATA"),
    col("value").substr(68, 19).alias("OPREFERENCIA"),
    col("value").substr(87, 22).alias("MOVOPREFERENCIA"),
    col("value").substr(109, 1).alias("MOVOPTIPO"),
    col("value").substr(110, 105).alias("BRANCOS")
)

print("Amostra de Registros Tipo D:")
display(df_liq_D.limit(5))

print("Amostra de Registros Tipo M:")
display(df_liq_M.limit(5))

# COMMAND ----------

#AJUSTE DOS DATAFRAMES PARA MESMO LAYOUT E JUNÇÃO (DUEOF, LIQ_D, LIQ_M)

# Lista de tipos para o DUEOF
tipos_dueof_selecionados = [
    "01", "02", "03", "04", "05","06", "07", "08", "12", "13", "25", 
    "26", "27", "30", "31", "32", "33", "34", "35", "36", "37", "38", "46", "45"
]

# 1. Preparação do DUEOF (Filtrando os tipos específicos)
df_dueof_ready = df_dueof_estruturado.select(
    col("TIPO_DUEOF"),
    col("NUMEMPENHO"),
    col("VALOR"),
    col("DATA"),
    col("NUMPROCESSO")
).filter(col("TIPO_DUEOF").isin(tipos_dueof_selecionados))

# 2. Preparação do Liq_D (Todas as instâncias)
df_liq_D_ready = df_liq_D.select(
    col("TIPO_DUEOF"), 
    col("NUMEMPENHO"),
    col("VALOR"),
    col("DATA"),
    lit(None).alias("NUMPROCESSO")
)

# 3. Liq_M: Filtrando movimentos 6 e 7 e concatenando TIPO_DUEOF e TIPOMOVIMENTO
df_liq_M_ready = df_liq_M.select(
    concat(col("TIPO_DUEOF"), col("TIPOMOVIMENTO")).alias("TIPO_DUEOF"),
    col("NUMEMPENHO"),
    col("VALOR"),
    col("DATA"),
    lit(None).alias("NUMPROCESSO")
).filter(col("TIPOMOVIMENTO").isin("6", "7"))

# 4. CONSOLIDAÇÃO FINAL (Union das 3 fontes)
df_final_linhas = df_dueof_ready \
    .union(df_liq_D_ready) \
    .union(df_liq_M_ready)

# 5. --- ETAPA DE ETL: CORREÇÃO DE TIPOS ---
df_final_linhas = df_final_linhas.withColumn(
    # lpad garante que datas como 02012026 não percam o '0' à esquerda se forem tratadas como número
    "DATA", to_date(lpad(col("DATA").cast("string"), 8, '0'), "ddMMyyyy")
).withColumn(
    # DecimalType(18, 2) resolve o problema das casas decimais "pulando" no Excel
    "VALOR", col("VALOR").cast(DecimalType(18, 2))
)

# 6. Ordenação Lógica
df_final_linhas = df_final_linhas.orderBy("NUMEMPENHO", "DATA")

# Exibição do resultado
display(df_final_linhas.limit(5))

# COMMAND ----------

#DATAFRAME DOTACAO AGREGADO (df_dotacao)
dotacao_subtrai = ["02", "13", "33", "36", "38", "45"]
dotacao_soma = ["01", "12", "25", "34", "35", "37", "32", "46"]


df_dotacao = df_final_linhas \
    .withColumn("NUMDOTACAO", substring(col("NUMEMPENHO"), 1, 11)) \
    .groupBy("NUMDOTACAO") \
    .agg(
        
        sum(when(col("TIPO_DUEOF") == "32", col("VALOR")).otherwise(0)).alias("DOTACAO_INICIAL"),
        (
            sum(when(col("TIPO_DUEOF").isin(dotacao_soma), col("VALOR")).otherwise(0)) - 
            sum(when(col("TIPO_DUEOF").isin(dotacao_subtrai), col("VALOR")).otherwise(0))
        ).alias("DOTACAO_ATUALIZADA"),
        (
        sum(when(col("TIPO_DUEOF") == "03", col("VALOR")).otherwise(0)) + 
        sum(when(col("TIPO_DUEOF") == "31", col("VALOR")).otherwise(0)) +
        sum(when(col("TIPO_DUEOF") == "07", col("VALOR")).otherwise(0)) - 
        sum(when(col("TIPO_DUEOF") == "04", col("VALOR")).otherwise(0)) -
        sum(when(col("TIPO_DUEOF") == "30", col("VALOR")).otherwise(0))
        ).alias("DOTACAO_EMPENHADA"), #todos empenhos da dotação
        ((
            sum(when(col("TIPO_DUEOF").isin(dotacao_soma), col("VALOR")).otherwise(0)) - 
            sum(when(col("TIPO_DUEOF").isin(dotacao_subtrai), col("VALOR")).otherwise(0))
        ) -
        (
        sum(when(col("TIPO_DUEOF") == "03", col("VALOR")).otherwise(0)) + 
        sum(when(col("TIPO_DUEOF") == "31", col("VALOR")).otherwise(0)) +
        sum(when(col("TIPO_DUEOF") == "07", col("VALOR")).otherwise(0)) - 
        sum(when(col("TIPO_DUEOF") == "04", col("VALOR")).otherwise(0)) -
        sum(when(col("TIPO_DUEOF") == "30", col("VALOR")).otherwise(0))
        )).alias("DOTACAO_A_EMPENHAR"),
        min(when(col("TIPO_DUEOF") == "32", col("DATA"))).alias("DATA")   
        ) 

display(df_dotacao.limit(5))

# COMMAND ----------

# 1. Primeiro, criamos a agregação por NUMEMPENHO (os valores específicos do empenho)
df_empenho_agregado = df_final_linhas.groupBy("NUMEMPENHO").agg(
    (
        sum(when(col("TIPO_DUEOF") == "05", col("VALOR")).otherwise(0)) + 
        sum(when(col("TIPO_DUEOF") == "27", col("VALOR")).otherwise(0)) +
        sum(when(col("TIPO_DUEOF") == "08", col("VALOR")).otherwise(0)) - 
        sum(when(col("TIPO_DUEOF") == "26", col("VALOR")).otherwise(0)) -
        sum(when(col("TIPO_DUEOF") == "06", col("VALOR")).otherwise(0)) 
    ).alias("LIQUIDACAO_PAGO"), #saldo de ordem de pagamento
   (
        sum(when(col("TIPO_DUEOF") == "D", col("VALOR")).otherwise(0)) + 
        sum(when(col("TIPO_DUEOF") == "M6", col("VALOR")).otherwise(0)) - 
        sum(when(col("TIPO_DUEOF") == "M7", col("VALOR")).otherwise(0))
    ).alias("LIQUIDADO"), #saldo de liquidação
    (
        sum(when(col("TIPO_DUEOF") == "03", col("VALOR")).otherwise(0))
    ).alias("EMPENHO_INICIAL"), #primeiro empenho
    (
        sum(when(col("TIPO_DUEOF") == "03", col("VALOR")).otherwise(0)) + 
        sum(when(col("TIPO_DUEOF") == "31", col("VALOR")).otherwise(0)) +
        sum(when(col("TIPO_DUEOF") == "07", col("VALOR")).otherwise(0)) - 
        sum(when(col("TIPO_DUEOF") == "04", col("VALOR")).otherwise(0)) -
        sum(when(col("TIPO_DUEOF") == "30", col("VALOR")).otherwise(0))
    ).alias("EMPENHO_ATUALIZADO"), #empenho atualizado
    ((
        sum(when(col("TIPO_DUEOF") == "D", col("VALOR")).otherwise(0)) + 
        sum(when(col("TIPO_DUEOF") == "M6", col("VALOR")).otherwise(0)) - 
        sum(when(col("TIPO_DUEOF") == "M7", col("VALOR")).otherwise(0))
    ) - (
        sum(when(col("TIPO_DUEOF") == "05", col("VALOR")).otherwise(0)) + 
        sum(when(col("TIPO_DUEOF") == "27", col("VALOR")).otherwise(0)) +
        sum(when(col("TIPO_DUEOF") == "08", col("VALOR")).otherwise(0)) - 
        sum(when(col("TIPO_DUEOF") == "26", col("VALOR")).otherwise(0)) -
        sum(when(col("TIPO_DUEOF") == "06", col("VALOR")).otherwise(0))
    )).alias("LIQUIDACAO_A_PAGAR"), # LIQUIDADO - LIQUIDACAO_PAGO
    ((
        sum(when(col("TIPO_DUEOF") == "03", col("VALOR")).otherwise(0)) + 
        sum(when(col("TIPO_DUEOF") == "31", col("VALOR")).otherwise(0)) +
        sum(when(col("TIPO_DUEOF") == "07", col("VALOR")).otherwise(0)) - 
        sum(when(col("TIPO_DUEOF") == "04", col("VALOR")).otherwise(0)) -
        sum(when(col("TIPO_DUEOF") == "30", col("VALOR")).otherwise(0))
    )-(
        sum(when(col("TIPO_DUEOF") == "D", col("VALOR")).otherwise(0)) + 
        sum(when(col("TIPO_DUEOF") == "M6", col("VALOR")).otherwise(0)) - 
        sum(when(col("TIPO_DUEOF") == "M7", col("VALOR")).otherwise(0))
    )).alias("EMPENHO_A_LIQUIDAR") # EMPENHO_ATUALIZADO - LIQUIDADO


)

# 2. Criamos a chave de ligação (11 dígitos) no dataframe de empenhos
df_empenho_com_chave = df_empenho_agregado.withColumn("CHAVE_DOTACAO", substring(col("NUMEMPENHO"), 1, 11))

# 3. O JOIN Mágico: Cruzamos os empenhos com o df_dotacao 
df_relatorio_final = df_empenho_com_chave.join(
    df_dotacao, 
    df_empenho_com_chave.CHAVE_DOTACAO == df_dotacao.NUMDOTACAO, 
    "left"
)

# 4. Seleção final das colunas para ficar limpo
df_final = df_relatorio_final.select(
    "NUMEMPENHO",
    "NUMDOTACAO",
    "DOTACAO_INICIAL",  
    "DOTACAO_ATUALIZADA", 
    "DOTACAO_EMPENHADA",  
    "DOTACAO_A_EMPENHAR", 
    "EMPENHO_INICIAL", 
    "EMPENHO_ATUALIZADO", 
    "EMPENHO_A_LIQUIDAR", 
    "LIQUIDADO",
    "LIQUIDACAO_PAGO",
    "LIQUIDACAO_A_PAGAR"    
).orderBy("NUMEMPENHO")

# Visualização
display(df_final.limit(10))

# COMMAND ----------

# EXPORTAÇÃO DAS TABELAS EM CSV

# 1. Configurações de Caminho e Credencial Global
container_silver = "silver-dados-processados"
base_path = f"wasbs://{container_silver}@{storage_account_name}.blob.core.windows.net/tabelas_csv"

# ISSO AQUI É A CHAVE: Define a permissão para todo o notebook de uma vez
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", 
    storage_account_key
)

# 2. NOMES PERSONALIZADOS
export_map = {
    "df_final_linhas": "base_eventos_linhas",
    "df_dotacao": "dotacao_consolidada",
    "df_final": "painel_empenhos_consolidado"
}

print("Iniciando exportação com nomes fixos e credenciais globais...")

for df_origem, nome_personalizado in export_map.items():
    temp_folder = f"{base_path}/{nome_personalizado}_temp"
    final_file_path = f"{base_path}/{nome_personalizado}.csv"
    
    # Pega o DataFrame real pelo nome
    df_para_gravar = globals()[df_origem]
    
    print(f"Gerando arquivo: {nome_personalizado}.csv")
    
    # Etapa 1: Grava como pasta (agora a credencial já está no sistema)
    df_para_gravar.coalesce(1).write.format("csv") \
      .mode("overwrite") \
      .option("header", "true") \
      .option("delimiter", ";") \
      .save(temp_folder)
    
    # Etapa 2: Localiza o arquivo e renomeia
    # Agora o dbutils terá acesso porque configuramos o spark.conf.set acima
    files = dbutils.fs.ls(temp_folder)
    csv_temp_file = [f.path for f in files if f.path.endswith(".csv")][0]
    
    dbutils.fs.cp(csv_temp_file, final_file_path)
    dbutils.fs.rm(temp_folder, recurse=True)

print("--- Sucesso! Arquivos nomeados corretamente no Azure. ---")