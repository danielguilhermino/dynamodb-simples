# dynamodb-simples

Classe para facilitar a abstração e comunicação com a instância do DynamoDB.

<h3>Um exemplo básico de como pode ser utilizado:</h3>

from database import db

base = db()

usuarios = base.pegarTudo(tabela='Usuarios')

print(usuarios)
