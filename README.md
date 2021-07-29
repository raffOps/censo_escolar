# ETL censo escolar

Esse projeto implementa um pipeline de ETL para os dados do [Censo Escolar](https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/censo-escolar)
utilizando Google Cloud Plataform. 

Os principais recursos da GCP utilizados nesse projeto são o [Composer (Airflow)](https://cloud.google.com/composer),
[GKE (Kubernetes)](https://cloud.google.com/kubernetes-engine), [Dataproc (Spark)](https://cloud.google.com/dataproc)
e [BigQuery](https://cloud.google.com/bigquery). Uma [conta GCP recém criada](https://cloud.google.com/free)
consegue rodar gratuitamente esse projeto. 

O GKE, Dataproc e BigQuery são provisionados pelo Composer. 
O Composer, por sua vez, é provisionado pelo fantástico [Terraform](https://www.terraform.io/). 

O deploy é realizado com poucos comandos no Google Cloud Shell, não necessitando fazer nada na sua máquina local.

## Dados 
#### [Download](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar)
Para esse projeto foram coletados os dados de 2011 até 2020. Os dados se dividem em 9 arquivos .CSVs: 5 para matriculas (1 por região do BR),
5 para docentes (1 por região do BR), e 1 arquivo cada para gestores, turmas e escolas

## Deploy

1. Faça um fork desse projeto para a sua conta Github.
1. Crie um [projeto](https://console.cloud.google.com/cloud-resource-manager) GCP novo. Nomeie levando em conta um nome que também será utilizado também pelo GCS bucket onde será armezanado o data lake do projeto.
2. Acesse [Google Cloud Build](https://console.cloud.google.com/cloud-build/triggers) e crie uma conexão com seu repositório Github.
3. Entre no Google Cloud Shell. O ícone dele está na parte superior do console, próximo a foto da sua conta Google. 
   No aba do Cloud Shell clique no botão ```Abrir editor``` para abrir o Visual Studio Code.
4. No Visual Studio Code abra um terminal e clone o fork que você realizou desse projeto.
5.  No arquivo ```~/etl_censo_escolar/infra/variables.tf``` defina o nome do projeto que você criou no passo 2 
    e o seu usuário Github. 
6. Vá para pasta etl do projeto: ```cd ~/etl_censo_escolar/infra```
7. Execute ```bash enable_api.sh```. Isso irá ativar as APIs do GCP utilizadas por esse projeto.
8. Instale os plugins Terraform executando ```terraform init```
9. Execute ```terraform apply``` e digite yes para aceitar. Isso irá começar o deploy do projeto.
10. Após o deploy do último componente (Composer), execute  ```terraform apply``` novamente. 
    Isso é necessário porque no primeiro apply foi baixado um arquivo no mesmo caminho de build de uma imagem Docker. O terraform buga, e
    não constrói a imagem, mas consegue construir no segundo apply.
    
11. Ao final de apply será printado o link de acesso do Airflow da GCP, o Composer.
12. O Composer é sincronizado com o seu repositório Github. A sincronização acontece sempre que haver um commit no repositório remoto.
Portanto, faça um commit e um push das suas alterações.
    
13. Deploy finalizado! 

