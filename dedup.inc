# PATH´s de processamento

export RAIZ="/bases/fiadmin2/migration/biblioref/DeDup"

VTELL="100000"

#echo "<MySQL server> Servidor do MySQL de origem dos registros - FI-ADMIN"
#OLD_mysqlserver="200.10.179.54"
mysqlserver="osmio11.bireme.br"
mysqlport="6612"
#echo "<MySQL user> usuario do servidor de origem"
#OLD_servername="fi_admin"
servername="acesso_fiadmin"
#echo "<MySQL password> senha do servidor de origem"
#OLD_serverpassword="fi_admin@BIREME"
serverpassword="fi_admin@BIR202309"
#echo " <MySQL database> base do servidor de origem"
serverdatabase="fi_admin"
#echo "<MySQL sql file list> Servidor de destino onde fica o indice do DeDup - Homolo e Prod" 

#AQUI ESCOLHER#
#urlhomolog="http://serverofi5.bireme.br:8180/DeDup/services"
#urldedup="https://dedup.bireme.org/services"
#urldedup="http://serverofi5.bireme.br:8180/DeDup/services"
#echo "Diretorio onde ficam os Schemas"
export DIRSQL="/home/javaapps/sbt-projects/DCDup/sql"
#echo "Diretorio dos scripts do DeDup"
#export DEDUP="/home/javaapps/sbt-projects/DCDup/bin/remote"
#export DEDUP_OFI5="/home/javaapps/sbt-projects/DCDup/serverofi5/DCDup/bin/local"
export DEDUP="/home/javaapps/sbt-projects/DCDup/bin"
export DEDUPBASE="/home/javaapps/DeDup"
export DEDUPWORK="/home/javaapps/DeDup/work"
export DEDUPSCHEMAS="/home/javaapps/DeDup/schemas"

# servidor de producao
export SERVER_PROD="diamante15.bireme.br"
export SERVER_PROD_PORT="8022"
export SERVER_PROD_USER="operacao"

# utilizado em migra_versao

export DIRINSUMO="/bases/fiadmin2/migration/biblioref/data_processing/work"
export DIRDATA="$RAIZ/data"
export DIRWRK="$RAIZ/work"
export DIROUTS="$RAIZ/outs"
export DIRTAB="/bases/fiadmin2/exec/tabs/biblioref"
export DIRSCRI="/bases/fiadmin2/exec/migration/biblioref/Dedup"

export DIRSH="/home/javaapps/sbt-projects/DCDup/bin"
export GPIPE="$DIRSH/geraPipeFile"

export DIRISIS="/usr/local/bireme/cisis/5.7c/linux64/ffiG_1024K-256"
export DIRLIND="/usr/local/bireme/cisis/5.7c/linux64/lindG4"

# Verificando diretorios do processamento
if [ "${DEDUP_VALIDATE_PATHS:-1}" = "1" ]; then
  if [ ! -d "$RAIZ" ]; then echo "ERROR: path nao existe : $RAIZ"; exit 0; fi
  if [ ! -d "$DIRINSUMO" ]; then echo "ERROR: path nao existe : $DIRINSUMO"; exit 0; fi
  if [ ! -d "$DEDUP" ]; then echo "ERROR: path nao existe : $DEDUP"; exit 0; fi
  if [ ! -d "$DIRSQL" ]; then echo "ERROR: path nao existe : $DIRSQL"; exit 0; fi
fi
