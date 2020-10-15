# CQLDUMP #

CQLDUMP surgiu da necessidade de extração de dump do Apache Cassandra DB sem a obrigação de extrair todo o seu conteúdo que em geral pode ser muito grande.

Ele permite o uso da cláusula where para filtrar e limitar a quantidade de registros que serão exportados para o dump.

O dump está pronto para ser importado através do source do Cassandra, já incluindo os comandos de criação do Keyspace e da Table exportada.

### Detalhes ###

* Desenvolvido em Python
* Versão: 0.0.1

### Instalação ###

* Faça o clone deste repositório
```bash

    $ git clone git@bitbucket.org:getrak/cqldump.git

```
* Entre no diretório clonado
```bash

    $ cd cqldump

```
* Instale o Utilitário
```bash

    $ sudo python3 setup.py install

```
Obs: É necessário possuir o Python instalado, para mais informações sobre como fazer a instalação acesse o link: https://www.python.org/downloads/

### Como usar ###

```bash

    Usage: cqldump [host] [keyspace] [table] [options ...]  > [nome_do_arquivo.cql]

    Options:

		--u, --user		Username
		--p, --password	Password
		--ssl			Path do arquivo chave para conexão SSL
		--w, --where	Cláusula Where
		-h, --help     output usage information
        
```