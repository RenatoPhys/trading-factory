"""
Script para monitorar todos os arquivos .json no diretório atual
e gerar arquivos .csv com os resultados de trading do MT5
"""

import os
import sys
import glob
import json
import datetime as dt
from pathlib import Path
import pandas as pd
import numpy as np
import MetaTrader5 as mt5
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()


#################################
###   Funções Importadas      ###
#################################

def get_mt5_credentials():
    """Carrega credenciais do MT5 do arquivo .env"""
    return {
        'login': int(os.getenv('MT5_LOGIN', 0)),
        'password': os.getenv('MT5_PASSWORD', ''),
        'server': os.getenv('MT5_SERVER', ''),
        'path': os.getenv('MT5_PATH', r"C:\Program Files\MetaTrader 5\terminal64.exe")
    }


def load_config(config_file):
    """Carrega todas as configurações do arquivo JSON"""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print(f"Arquivo {config_file} não encontrado!")
        return None
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON do arquivo {config_file}")
        return None
    except Exception as e:
        print(f"Erro ao carregar {config_file}: {e}")
        return None


def connect_mt5(config=None):
    """Estabelece conexão com MT5"""
    if config is None:
        config = get_mt5_credentials()
    
    # Validar credenciais
    if not config['login'] or not config['password'] or not config['server']:
        print("Erro: Credenciais do MT5 não encontradas no arquivo .env")
        print("Certifique-se de que MT5_LOGIN, MT5_PASSWORD e MT5_SERVER estão definidos")
        return False
    
    if not mt5.initialize(
        login=config['login'], 
        server=config['server'], 
        password=config['password'], 
        path=config['path']
    ):
        print("initialize() failed, error code =", mt5.last_error())
        return False
    
    print('Ligado ao MT5 com sucesso!')
    print(f'Conta: {config["login"]} | Servidor: {config["server"]}')
    print('-' * 20)
    return True


def get_timeframe_offset(timeframe):
    """Retorna o offset em minutos baseado no timeframe"""
    timeframe_map = {
        't1': 1, 't2': 2, 't5': 5, 't10': 10, 't15': 15, 't30': 30,
        'h1': 60, 'h4': 240, 'd1': 1440,
    }
    return timeframe_map.get(timeframe.lower(), 5)


def trade_report(symbol, data_ini, data_fim, cost_per_lot=0.25):
    """Extrai relatório de trades do MT5"""
    deals = mt5.history_deals_get(data_ini, data_fim, group=symbol)
    
    if deals is None or len(deals) == 0:
        print(f"Nenhum deal encontrado para {symbol} no período especificado")
        return pd.DataFrame()
    
    print(f"Encontrados {len(deals)} deals para {symbol}")
    
    df = pd.DataFrame(list(deals), columns=deals[0]._asdict().keys())
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df['custo'] = df['volume'] * cost_per_lot
    df['lucro'] = df['profit'] - df['custo']
    
    return df


def process_trades_data(dfmt5, magic_number, cost_per_lot=0.5, timeframe='t5'):
    """Processa dados de trades separando entradas e saídas"""

    if dfmt5.empty:
        print("DataFrame vazio - nenhum trade para processar")
        return pd.DataFrame()
    
    # Dataframe com entradas (trades com 'patt' no comentário)
    dfmt5_ent = dfmt5[dfmt5['magic']==magic_number][
        ['time', 'type', 'position_id', 'magic', 'price', 'volume']
    ].copy()
    
    if dfmt5_ent.empty:
        print("Nenhuma entrada encontrada com padrão 'patt'")
        return pd.DataFrame()
    
    dfmt5_ent.rename(columns={'price': 'price_ent', 'time': 'time_ent'}, inplace=True)
    dfmt5_ent.loc[dfmt5_ent['type'] == 1, 'posi'] = 'short'
    dfmt5_ent.loc[dfmt5_ent['type'] == 0, 'posi'] = 'long'
    
    # Dataframe com saídas (trades sem 'patt' no comentário)
    dfmt5_ext = dfmt5[dfmt5['magic']==0][
        ['time', 'position_id', 'price', 'profit', 'comment']
    ].copy()
    
    if dfmt5_ext.empty:
        print("Nenhuma saída encontrada")
        return pd.DataFrame()
    
    dfmt5_ext.rename(columns={'price': 'price_ext', 'time': 'time_ext'}, inplace=True)
    
    # Unindo entradas e saídas
    dfmt5_2 = pd.merge(dfmt5_ent, dfmt5_ext, on="position_id", how='left')
    
    if dfmt5_2.empty:
        print("Nenhum trade completo encontrado após merge")
        return pd.DataFrame()
    
    # Calculando métricas
    dfmt5_2['delta_t'] = (dfmt5_2['time_ext'] - dfmt5_2['time_ent']).dt.total_seconds() / 60
    dfmt5_2['pts_final_demo'] = abs(dfmt5_2['price_ext'] - dfmt5_2['price_ent'])
    
    # Pontos finais considerando direção
    dfmt5_2.loc[dfmt5_2['posi'] == 'long', 'pts_final_real'] = dfmt5_2['price_ext'] - dfmt5_2['price_ent']
    dfmt5_2.loc[dfmt5_2['posi'] == 'short', 'pts_final_real'] = dfmt5_2['price_ent'] - dfmt5_2['price_ext']
    
    # Ajustando tempo para comparação com candles baseado no timeframe
    timeframe_offset = get_timeframe_offset(timeframe)
    dfmt5_2['time'] = dfmt5_2['time_ent'] - pd.Timedelta(minutes=timeframe_offset)
    dfmt5_2['time'] = dfmt5_2['time'].dt.round('min')
    dfmt5_2.set_index("time", inplace=True)
    
    # Lucro com custo
    dfmt5_2['lucro'] = dfmt5_2['profit'] - dfmt5_2['volume'] * cost_per_lot
    
    # Filtrando por magic number
    dfmt5_2 = dfmt5_2[dfmt5_2['magic'] == magic_number].copy()
    
    if dfmt5_2.empty:
        print(f"Nenhum trade encontrado para magic number {magic_number}")
        return pd.DataFrame()
    
    # Lucro acumulado
    dfmt5_2['cstrategy'] = dfmt5_2['lucro'].cumsum()

    
    return dfmt5_2


def base_trades(config_file, data_fim=None, symbol_override=None):
    """Função principal para extrair e processar dados de trading do MT5"""
    
    # Carregar configurações
    config = load_config(config_file)
    if config is None:
        return pd.DataFrame()
    
    # Extrair parâmetros de data do config
    data_ini = config.get('data_ini', '2025-06-25')
    
    # Usar data fim fornecida, do config ou data atual
    if data_fim is None:
        data_fim = config.get('data_fim', dt.datetime.now().strftime('%Y-%m-%d'))
    
    print(f"Período de análise: {data_ini} a {data_fim}")
    
    # Determinar símbolo e normalizar para WIN
    symbol = symbol_override if symbol_override else config.get('symbol', 'WIN')
    
    # SOLUÇÃO SIMPLES: Se contém WIN, usar *WIN*
    if "WIN" in symbol.upper():
        symbol2 = symbol.upper()
        symbol = "*WIN*"      
    if "WDO" in symbol.upper():
        symbol2 = symbol.upper()
        symbol = "*WDO*"     
    elif not symbol.startswith('*'):
        symbol = f"*{symbol}*"
    
    magic_number = config.get('magic_number', 2)
    cost_per_lot = config.get('cost_per_lot', 0.5)
    timeframe = config.get('timeframe', 't5')
    strategy_name = config['strategy']
    
    print(f"Símbolo: {symbol}")
    print(f"Magic Number: {magic_number}")
    print(f"Custo por lote: {cost_per_lot}")
    print(f"Timeframe: {timeframe}")
    
    # Extrair dados do MT5
    dfmt5 = trade_report(
        symbol, 
        pd.Timestamp(data_ini), 
        pd.Timestamp(data_fim) + dt.timedelta(days=1),
        cost_per_lot
    )
    
    if dfmt5.empty:
        print("Nenhum dado encontrado no MT5")
        return pd.DataFrame()
    
    # Processar dados
    result_df = process_trades_data(dfmt5, magic_number, cost_per_lot, timeframe)
    
    if not result_df.empty:
        # Criar diretório se não existir
        os.makedirs("real_results", exist_ok=True)
        
        # Para o nome do arquivo, usar apenas WIN se for contrato WIN
        file_symbol = "WIN" if "*WIN*" in symbol else symbol.replace('*', '')
        output_file = f"controle/real_results/results_{symbol2}_{timeframe}_{strategy_name}_magic_{magic_number}.csv"
        result_df.to_csv(output_file, index=True)
        print(f"Resultados salvos em: {output_file}")
    
    return result_df


#################################
###   Funções do Monitor      ###
#################################

def find_json_configs():
    """Encontra todos os arquivos .json no diretório do script ou diretório atual"""
    
    # Primeiro, tenta no diretório onde o script está localizado
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Diretório do script: {script_dir}")
    print(f"Diretório de trabalho atual: {os.getcwd()}")
    
    # Verificar se há arquivos JSON no diretório do script
    script_files = os.listdir(script_dir)
    script_json_pattern = os.path.join(script_dir, "*.json")
    script_json_files = glob.glob(script_json_pattern)
    
    print(f"Arquivos no diretório do script: {script_files}")
    print(f"Arquivos .json no diretório do script: {[os.path.basename(f) for f in script_json_files]}")
    
    if script_json_files:
        print("Usando arquivos JSON do diretório do script")
        return sorted(script_json_files)
    
    # Se não encontrou no diretório do script, verifica se existe pasta controle no working directory
    controle_dir = "controle"
    if os.path.exists(controle_dir):
        print(f"Verificando pasta controle em: {os.path.join(os.getcwd(), controle_dir)}")
        controle_files = os.listdir(controle_dir)
        controle_json_pattern = os.path.join(controle_dir, "*.json")
        controle_json_files = glob.glob(controle_json_pattern)
        
        print(f"Arquivos em controle/: {controle_files}")
        print(f"Arquivos .json em controle/: {[os.path.basename(f) for f in controle_json_files]}")
        
        if controle_json_files:
            print("Usando arquivos JSON da pasta controle")
            return sorted(controle_json_files)
    
    # Por último, tenta no diretório atual
    current_files = os.listdir('.')
    current_json_files = glob.glob("*.json")
    
    print(f"Arquivos no diretório atual: {current_files}")
    print(f"Arquivos .json no diretório atual: {current_json_files}")
    
    return sorted(current_json_files)


def process_all_configs(data_fim=None):
    """Processa todos os arquivos .json encontrados no diretório atual"""
    
    print("="*50)
    print("MONITOR DE CONFIGURAÇÕES")
    print("="*50)
    print(f"Diretório de trabalho: {os.getcwd()}")
    print(f"Buscando arquivos JSON em: controle/")
    
    # Encontrar todos os arquivos .json no diretório atual
    json_files = find_json_configs()
    
    if not json_files:
        print("Nenhum arquivo .json encontrado no diretório atual")
        return
    
    print(f"\nEncontrados {len(json_files)} arquivo(s) .json:")
    for i, file in enumerate(json_files, 1):
        print(f"  {i}. {file}")
    
    print("\n" + "-"*50)
    
    # Conectar ao MT5
    print("Conectando ao MT5...")
    if not connect_mt5():
        print("Erro: Não foi possível conectar ao MT5")
        return
    
    # Processar cada arquivo
    sucessos = 0
    falhas = 0
    
    for i, config_file in enumerate(json_files, 1):
        print(f"\n[{i}/{len(json_files)}] Processando: {config_file}")
        print("-" * 40)
        
        try:
            df_results = base_trades(config_file=config_file, data_fim=data_fim)
            
            if not df_results.empty:
                print(f"[OK] {config_file} - {len(df_results)} trades processados")
                sucessos += 1
            else:
                print(f"[AVISO] {config_file} - Nenhum trade encontrado")
                falhas += 1
                
        except Exception as e:
            print(f"[ERRO] {config_file}: {str(e)}")
            falhas += 1
    
    # Resumo
    print("\n" + "="*50)
    print("RESUMO")
    print("="*50)
    print(f"Sucessos: {sucessos}")
    print(f"Falhas: {falhas}")
    
    # Desconectar do MT5
    mt5.shutdown()
    print("Desconectado do MT5")


def main():
    """Função principal"""
    print("Iniciando monitor de configurações...")
    process_all_configs()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nProcessamento interrompido pelo usuário")
        try:
            mt5.shutdown()
        except:
            pass
    except Exception as e:
        print(f"Erro: {e}")
        try:
            mt5.shutdown()
        except:
            pass