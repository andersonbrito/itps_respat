## Instalação

Para instalar este pipeline, instale `conda` e `mamba`:

- Instalação do Conda: [clique aqui](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)

- Instalação do Mamba, execute este comando, uma vez que Conda esteja instalado:
```
conda install -n base conda-forge::mamba
```

Agora clone este repositório:
Clone this repository `ncov`
```
git clone https://github.com/andersonbrito/itps_sgtf.git
```

Uma vez instalados `conda` e `mamba`, acesse o diretório `config`, e execute os seguintes comandos para criar o ambiente conda `diag`:

```
 mamba create -n diag
 mamba env update -n diag --file environment.yaml
 ```

Por fim, ative o ambiente `diag`:
```
conda activate diag
```

## Execução

Para executar o pipeline até o último passo, execute o seguinte comando, com o ambiente `diag` ativado:
```
snakemake all --cores all
```
