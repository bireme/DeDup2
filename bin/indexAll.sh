#!/usr/bin/env bash

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
if [ -f /bases/fiadmin2/exec/settings/dedup.inc ]; then
  . /bases/fiadmin2/exec/settings/dedup.inc
fi

# -------------------------------------------------------------------------- #

# diretorio inicial
INITIAL_DIR=$PWD
PROJECT_DIR=/home/javaapps/sbt-projects/DeDup2/

# vai para o diretório onde serão gerados os índices
cd $PROJECT_DIR || exit
checkError "$?"  "vai para o diretorio base do processamento dos indices"

# processa os indices
tuplas=(
  "LILACS_Sas.sql LILACS_Sas_ingles.sql lilacs_Sas LILACS_Sas_Seven.cfg"
  "LIS.sql LIS LIS_Two.cfg"
  "LILACS_MNT.sql LILACS_MNT_ingles.sql lilacs_MNT LILACS_MNT_Four.cfg"
  "LILACS_MNTam.sql LILACS_MNTam_ingles.sql lilacs_MNTam LILACS_MNTam_Five.cfg"
  "DIREV.sql DIREV DIREV_Three.cfg"
  "LILACS_Sas_Source.sql lilacs_Sas_Source LILACS_Sas_Source.cfg"
)

# Geração dos índices a partir do MySql
for linha in "${tuplas[@]}"; do
  # Converte a linha em um array chamado 'campos'
  read -r -a campos <<< "$linha"

  split_document_field=""
  case "${campos[0]}" in
    LILACS_Sas.sql|LIS.sql|LILACS_MNTam.sql|DIREV.sql)
      split_document_field=" -splitDocumentField=title"
      ;;
  esac

  case ${#campos[@]} in
    3)  # se tiver só um arquivo *.sql
      sql1="sqls/${campos[0]}"
      index="indexes/${campos[1]}"
      schema="conf/${campos[2]}"

      echo
      echo "==== $index ==== [TIME-STAMP] `date '+%Y.%m.%d %H:%M:%S'`"
      echo "bin/MySQL2Lucene.sh -mySqlHost=$mysqlserver -mySqlPort=$mysqlport -mySqlUser=$servername -mySqlPassword=$serverpassword -mySqlDbname=$serverdatabase -sqlfs=$sql1 -index=$index -schema=$schema -fieldToIndex=title -jsonFieldFile=conf/jsonFields.txt$split_document_field"
      bin/MySQL2Lucene.sh -mySqlHost=$mysqlserver -mySqlPort=$mysqlport -mySqlUser=$servername -mySqlPassword=$serverpassword -mySqlDbname=$serverdatabase -sqls=$sql1 -index=$index -schema=$schema -fieldToIndex=title -jsonFieldFile=conf/jsonFields.txt $split_document_field
      ret="$?"
      if [ "$ret" -ne 0 ]; then
        sendemail -f appofi@bireme.org -u "DeDup Service - index creation ERROR - $(date '+%Y%m%d')" -m "DeDup Service - Erro na criacao do indice $index." -t appofi@bireme.org -cc barbieri@paho.org -s esmeralda.bireme.br
      fi
      ;;

    4) # se tiver dois arquivos *.sql
      sql1="sqls/${campos[0]}"
      sql2="sqls/${campos[1]}"
      index="indexes/${campos[2]}"
      schema="conf/${campos[3]}"

      echo
      echo "==== $index ===="
      echo "bin/MySQL2Lucene.sh -mySqlHost=$mysqlserver -mySqlPort=$mysqlport -mySqlUser=$servername -mySqlPassword=$serverpassword -mySqlDbname=$serverdatabase -sqls=$sql1,$sql2 -index=$index -schema=$schema -fieldToIndex=title -jsonFieldFile=conf/jsonFields.txt$split_document_field"
      bin/MySQL2Lucene.sh -mySqlHost=$mysqlserver -mySqlPort=$mysqlport -mySqlUser=$servername -mySqlPassword=$serverpassword -mySqlDbname=$serverdatabase -sqls=$sql1,$sql2 -index=$index -schema=$schema -fieldToIndex=title -jsonFieldFile=conf/jsonFields.txt $split_document_field
      ret="$?"
      if [ "$ret" -ne 0 ]; then
        sendemail -f appofi@bireme.org -u "DeDup Service - index creation ERROR - $(date '+%Y%m%d')" -m "DeDup Service - Erro na criacao do indice $index." -t appofi@bireme.org -cc barbieri@paho.org -s esmeralda.bireme.br
      fi
      ;;

    *) # se tiver mais parâmetros que o normal
      checkError "1" "chamada ao procedimento MySQL2Lucene.sh com mais parametros que o normal" 0
      ;;
  esac
done

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
