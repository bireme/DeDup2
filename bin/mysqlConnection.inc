# Dados de conexão ao MySQL usados pelos scripts bin/Gen*.sh e bin/indexAll.sh.
# Deve ser incluído (com ".") depois do dedup.inc.
#
# Servidor, porta e base vêm do dedup.inc, quando existir; senão, valem os
# valores padrão abaixo. Usuário e senha NÃO passam pela linha de comando:
# SQL2CSV/SQL2Lucene os leem de conf/credenciais.txt (perfil "fiadmin"), o
# mesmo arquivo usado pelos conf/*.cfg. Outro arquivo pode ser indicado com a
# variável DEDUP_CREDENTIALS.
# Define: mysqlserver, mysqlport, serverdatabase.

mysqlserver="${mysqlserver:-osmio11.bireme.br}"
mysqlport="${mysqlport:-6612}"
serverdatabase="${serverdatabase:-fi_admin}"

dedup_credentials_file="${DEDUP_CREDENTIALS:-$(dirname "${BASH_SOURCE[0]}")/../conf/credenciais.txt}"
if [ ! -r "$dedup_credentials_file" ]; then
  echo "ERROR: arquivo de credenciais não encontrado: $dedup_credentials_file (ver conf/credenciais.txt.example)" >&2
  unset dedup_credentials_file
  return 1 2>/dev/null || exit 1
fi
unset dedup_credentials_file
