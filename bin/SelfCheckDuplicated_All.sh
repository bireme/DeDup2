#!/bin/bash

cd /home/javaapps/sbt-projects/DeDup2/ || exit

# -------------------------------------------------------------------------- #

# Anota hora de inicio de processamento
export HORA_INICIO=`date '+ %s'`
export HI="`date '+%Y.%m.%d %H:%M:%S'`"

echo "[TIME-STAMP] `date '+%Y.%m.%d %H:%M:%S'`"
echo
# -------------------------------------------------------------------------- #

echo
echo "------------------------------------------------"
echo "Apagando arquivos de duplicados anteriores"
rm selfCheck/DIREV/*
rm selfCheck/LILACS_MNT/*
rm selfCheck/LILACS_MNTam/*
rm selfCheck/LILACS_Sas/*
rm selfCheck/LILACS_Sas_Source/*
rm selfCheck/LIS/*

echo
echo "------------------------------------------------"
echo "Procurando duplicados na base DIREV..."
bin/SelfCheckDuplicated_DIREV.sh

echo
echo "------------------------------------------------"
echo "Procurando duplicados na base Lilacs_Sas..."
bin/SelfCheckDuplicated_LILACS_Sas.sh

echo
echo "------------------------------------------------"
echo "Procurando duplicados na base Lilacs_Sas_Source..."
bin/SelfCheckDuplicated_LILACS_Sas_Source.sh

echo
echo "------------------------------------------------"
echo "Procurando duplicados na base LIS..."
bin/SelfCheckDuplicated_LIS.sh

echo
echo "------------------------------------------------"
echo "Procurando duplicados na base MNTam..."
bin/SelfCheckDuplicated_MNTam.sh

echo
echo "------------------------------------------------"
echo "Procurando duplicados na base MNT..."
bin/SelfCheckDuplicated_MNT.sh

# ---------------------------------------------------------------------------#

echo
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

cd - || exit

