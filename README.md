# Atualização semanal do DNE (Correios)

Este projeto baixa o arquivo oficial do DNE (Correios), extrai os logradouros e sincroniza a tabela `postcode_correios` no MySQL.

## Requisitos

- Python 3.10+
- Acesso ao MySQL (Aurora 8.0)
- Permissão de `LOAD DATA LOCAL INFILE`

## Instalação

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuração

Use variáveis de ambiente (ou passe argumentos no CLI):

```bash
export DNE_DB_HOST=seu-host
export DNE_DB_USER=seu-usuario
export DNE_DB_PASSWORD=sua-senha
export DNE_DB_NAME=seu-banco
export DNE_DB_PORT=3306
export DNE_TABLE=postcode_correios
```

## Executar (interface web)

```bash
./app.py
```

## Teste local completo (MySQL + app)

Este fluxo sobe um MySQL local, cria a tabela automaticamente e inicia a interface web.

```bash
docker compose up --build
```

Abra `http://localhost:5000` e clique em **Iniciar sincronização**.

## URL para teste

Depois do `docker compose up`, a URL local para testar é:

```
http://localhost:5000
```

## O que o script faz

1. Baixa o ZIP do DNE (`DNE_GU.zip`).
2. Extrai os arquivos `*_LOGRADOUROS.TXT`.
3. Carrega os registros em uma tabela de estágio.
4. Insere/apaga registros na tabela final.

## Interface web

Abra `http://localhost:5000` e clique em **Iniciar sincronização**. Os logs ficam visíveis na tela.

Opcionalmente, defina onde salvar os logs:

```bash
export DNE_LOG_PATH=logs/dne_sync.log
```

## Observações

- O parser usa o leiaute oficial do DNE (registro `D` de logradouros).
- Campos importados: `cep`, `street`, `city`, `region`, `neighborhood`.
- A sincronização faz apenas `INSERT` de novos CEPs e `DELETE` dos removidos.
