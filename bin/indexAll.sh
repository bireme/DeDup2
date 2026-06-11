#!/usr/bin/env bash

# errado
#sh /caminho/bin/indexAll.sh

# certo
#bash /caminho/bin/indexAll.sh

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
export JAVA_HOME=$JAVA_HOME_25

export PATH=$JAVA_HOME/bin:$PATH

# Obtém o nome de usuário efetivo, mesmo se $USER não estiver definido
if [[ "$(id -un)" != "operacao" ]]; then
  echo "Este script só pode ser executado pelo usuário 'operacao'."
  exit 1
fi


# -----------------------------------------------------------
# checkError <exit_code> [msg]
# Se exit_code ≠ 0 → dispara e-mail e exit -1
# -----------------------------------------------------------
checkError() {
  local rc="$1"      # código de retorno
  local errMsg="$2"  # mensagem de erro

  if [ "$rc" -eq 0 ]; then
    echo "$errMsg -- OK"
  else
    echo "$errMsg -- ERROR"
    sendemail -f appofi@bireme.org -u "DeDup Service - indexes creation ERROR - $(date '+%Y%m%d')" -m "DeDup Service - Erro na criacao dos indices. Mensagem: $errMsg" -t appofi@bireme.org -cc barbieri@paho.org -s esmeralda.bireme.br

    exit 1
  fi
}

# -------------------------------------------------------------------------- #

# Anota hora de inicio de processamento
export HORA_INICIO=`date '+ %s'`
export HI="`date '+%Y.%m.%d %H:%M:%S'`"

echo "[TIME-STAMP] `date '+%Y.%m.%d %H:%M:%S'`"
echo ""

# -------------------------------------------------------------------------- #
# Ajustando variaveis para processamento
. /bases/fiadmin2/exec/settings/dedup.inc

TOMCAT_DIR=$DEDUPBASE/apache-tomcat-9.0.105
# -------------------------------------------------------------------------- #

# diretorio inicial
INITIAL_DIR=$PWD

# vai para o diretório onde serão gerados os índices
cd $DEDUPBASE
checkError "$?"  "vai para o diretorio base do processamento dos indices"

# move diretório work para diretório work_old
if [[ -d "work_old" ]]; then
  rm -fr work_old
  checkError "$?" "apaga diretorio work_old"
fi  
if [[ -d "work" ]]; then
  mv work work_old
  checkError "$?" "move diretorio work para diretório work_old"
else
  mkdir work_old
  checkError "$?" "criando diretorio work_old"
fi

# cria diretório work
mkdir work
checkError "$?" "cria diretorio work"

# processa os indices
tuplas=(
  "LILACS_Sas.sql LILACS_Sas_ingles.sql lilacs_Sas configLILACS_Sas_Seven.cfg"
  "LIS.sql LIS configLIS_Two.cfg"
  "LILACS_MNT.sql LILACS_MNT_ingles.sql lilacs_MNT configLILACS_MNT_Four.cfg"
  "LILACS_MNTam.sql LILACS_MNTam_ingles.sql lilacs_MNTam configLILACS_MNTam_Five.cfg"
  "DIREV.sql DIREV configDIREV_Three.cfg"
  "LILACS_Sas_Source.sql lilacs_Sas_Source configLILACS_Sas_Source.cfg"
)

# Geração dos índices a partir do MySql
for linha in "${tuplas[@]}"; do
  # Converte a linha em um array chamado 'campos'
  read -r -a campos <<< "$linha"

  case ${#campos[@]} in
    3)  # se tiver só um arquivo *.sql
      sql1=${campos[0]}
      index=${campos[1]}
      schema=${campos[2]}

      echo
      echo "==== $index ==== [TIME-STAMP] `date '+%Y.%m.%d %H:%M:%S'`"
      echo "./MySQL2Lucene.sh -host=$mysqlserver -port=$mysqlport -user=$servername -pswd=$serverpassword -dbnm=$serverdatabase -sqls=$DIRSQL/$sql1 -index=$DEDUPWORK/$index -schema=$DEDUPSCHEMAS/$schema"
      $DEDUP/MySQL2Lucene.sh -host=$mysqlserver -port=$mysqlport -user=$servername -pswd=$serverpassword -dbnm=$serverdatabase -sqls=$DIRSQL/$sql1 -index=$DEDUPWORK/$index -schema=$DEDUPSCHEMAS/$schema
      ret="$?"
      if [ "$ret" -ne 0 ]; then
        sendemail -f appofi@bireme.org -u "DeDup Service - index creation ERROR - $(date '+%Y%m%d')" -m "DeDup Service - Erro na criacao do indice $index." -t appofi@bireme.org -cc barbieri@paho.org -s esmeralda.bireme.br
        if [[ -d "work_old/$index" ]]; then
          cp -pr ../work_old/$index work/
          checkError "$?" "copia do antigo indice para o diretorio atual"
        fi
      fi
      ;;

    4) # se tiver dois arquivos *.sql
      sql1=${campos[0]}
      sql2=${campos[1]}
      index=${campos[2]}
      schema=${campos[3]}

      echo
      echo "==== $index ===="
      echo "./MySQL2Lucene.sh -host=$mysqlserver -port=$mysqlport -user=$servername -pswd=$serverpassword -dbnm=$serverdatabase -sqls=$DIRSQL/$sql1,$DIRSQL/$sql2 -index=$DEDUPWORK/$index -schema=$DEDUPSCHEMAS/$schema"
      $DEDUP/MySQL2Lucene.sh -host=$mysqlserver -port=$mysqlport -user=$servername -pswd=$serverpassword -dbnm=$serverdatabase -sqls=$DIRSQL/$sql1,$DIRSQL/$sql2 -index=$DEDUPWORK/$index -schema=$DEDUPSCHEMAS/$schema
      ret="$?"
      if [ "$ret" -ne 0 ]; then
        sendemail -f appofi@bireme.org -u "DeDup Service - index creation ERROR - $(date '+%Y%m%d')" -m "DeDup Service - Erro na criacao do indice $index." -t appofi@bireme.org -cc barbieri@paho.org -s esmeralda.bireme.br
        if [[ -d "work_old/$index" ]]; then
          cp -pr ../work_old/$index work/
          checkError "$?" "copia do antigo indice para o diretório atual"
        fi
      fi
      ;;

    *) # se tiver mais parâmetros que o normal
      checkError "1" "chamada ao procedimento MySQL2Lucene.sh com mais parametros que o normal" 0
      ;;
  esac
done

# vai para o diretório raiz do projeto
cd $DEDUPBASE
checkError "$?"  "vai para o diretorio raiz do projeto - $DEDUPBASE"

# apaga diretório work_old
rm -fr work_old
checkError "$?" "apaga diretorio work_old"

# gera arquivo compactado contendo diretório work
tar -cvzpf work.tgz work
checkError "$?" "gera arquivo compactado contendo diretorio work"

# copia arquivo compactado para servidor de produção
scp -P $SERVER_PROD_PORT work.tgz "$SERVER_PROD_USER@$SERVER_PROD:$DEDUPBASE/"
checkError "$?" "copia arquivo compactado para servidor de producao"

# finaliza a execução do Tomcat
ssh -p $SERVER_PROD_PORT $SERVER_PROD_USER@$SERVER_PROD "$TOMCAT_DIR/bin/shutdown.sh"
checkError "$?" "finaliza a execucao do Tomcat"

# apaga diretorio work_old no servidor de producao se existir o diretori work
ssh -p $SERVER_PROD_PORT $SERVER_PROD_USER@$SERVER_PROD "[[ -d $DEDUPBASE/work ]] && rm -fr $DEDUPBASE/work_old"
checkError "$?" "apaga diretorio work_old no servidor de producao"

# move diretório work para work_old no servidor de produção
ssh -p $SERVER_PROD_PORT $SERVER_PROD_USER@$SERVER_PROD "[[ -d $DEDUPBASE/work ]] && mv $DEDUPBASE/work $DEDUPBASE/work_old"
checkError "$?" "move diretorio work para work_old no servidor de producao"

# descompacta arquivo compactado
ssh -p $SERVER_PROD_PORT $SERVER_PROD_USER@$SERVER_PROD "tar -xvzpf $DEDUPBASE/work.tgz --directory=$DEDUPBASE"
checkError "$?" "descompacta arquivo compactado"

# apaga arquivo compactado no servidor de produção
#ssh -p $SERVER_PROD_PORT $SERVER_PROD_USER@$SERVER_PROD "rm $DEDUPBASE/work.tgz"
#checkError "$?" "apaga arquivo compactado no servidor de producao"

# apaga diretório work_old no servidor de produção
#ssh -p "$SERVER_PROD_PORT" "$SERVER_PROD_USER@$SERVER_PROD" '[[ -d $DEDUPBASE/work_old ]] && rm -fr $DEDUPBASE/work_old'
#checkError "$?" "apaga diretorio work_old no servidor de producao"

# apaga arquivos write.lock dos indices
ssh -p $SERVER_PROD_PORT $SERVER_PROD_USER@$SERVER_PROD "find /home/javaapps/DeDup -name write.lock | xargs rm"
checkError "$?" "apaga arquivos write.lock dos indices"

# aguarda 1 minuto
echo "aguarda 1 minuto"
sleep 1m

# restart tomcat
ssh -p $SERVER_PROD_PORT $SERVER_PROD_USER@$SERVER_PROD "PATH=$JAVA_HOME_25/bin:$PATH;$TOMCAT_DIR/bin/startup.sh"
checkError "$?" "restart tomcat"

# checa se realmente o servico esta noar e funcionando bem
CONTENT="$(curl https://dedup.bireme.org/services/schemas)"
COUNT="$(echo $CONTENT | grep -c Source)"

if [ "$COUNT" eq 0 ]; then
   # envia email dizendo que o site nao esta executando corretamente
  sendemail -f appofi@bireme.org -u "DeDup in Tomcat is not working! - $(date '+%Y%m%d')" -m "The DeDup service check failed" -t appofi@bireme.org -s esmeralda.bireme.br
  checkError "$?" "envia email dizendo que o cheque do servico de DeDup no Tomcat falhou"
else	
  # envia email dizendo que o processo finalizou corretamente
  sendemail -f appofi@bireme.org -u "DeDup index creation finished successfully! - $(date '+%Y%m%d')" -m "Criacao dos indices do DeDup terminou sem erros." -t appofi@bireme.org -s esmeralda.bireme.br
  checkError "$?" "envia email dizendo que o processo finalizou corretamente"
fi

# ---------------------------------------------------------------------------#

echo
echo "Fim de processamento"
echo

HORA_FIM=`date '+ %s'`
DURACAO=`expr ${HORA_FIM} - ${HORA_INICIO}`
HORAS=`expr ${DURACAO} / 60 / 60`
MINUTOS=`expr ${DURACAO} / 60 % 60`
SEGUNDOS=`expr ${DURACAO} % 60`

echo
echo "DURACAO DE PROCESSAMENTO"
echo "-------------------------------------------------------------------------"
echo " - Inicio:  ${HI}"
echo " - Termino: `date '+%Y.%m.%d %H:%M:%S'`"
echo
echo " Tempo de execucao: ${DURACAO} [s]"
echo " Ou ${HORAS}h ${MINUTOS}m ${SEGUNDOS}s"
echo

# ------------------------------------------------------------------------- #
echo "[TIME-STAMP] `date '+%Y.%m.%d %H:%M:%S'` [:FIM:] Processa  ${0} ${1} ${2} ${3} ${4}"
# ------------------------------------------------------------------------- #
echo
echo

cd $INITIAL_DIR

