from pyspark import SparkContext, SparkConf

# Configuração do Spark
conf = SparkConf()
conf.setAppName("RemoveDuplicatesAndTopSalaries")
conf.setMaster("local")
sc = SparkContext(conf=conf)

# Carregar o dataset como um RDD
rdd = sc.textFile("caminho/do/seu/arquivo/dataset.csv")

# Remover linhas duplicadas
rdd_sem_duplicatas = rdd.distinct()

# Converter as linhas em tuplas (salario, linha) e ordenar pelo salario em ordem decrescente
rdd_salarios_ordenados = rdd_sem_duplicatas.map(lambda linha: (float(linha.split(',')[1]), linha)) \
                                             .sortByKey(ascending=False)

# Pegar os 6 maiores salários
seis_maiores_salarios = rdd_salarios_ordenados.take(6)

# Exibir os 6 maiores salários
for salario, linha in seis_maiores_salarios:
    print("Salário:", salario, "| Linha:", linha)

# Parar o SparkContext
sc.stop()
