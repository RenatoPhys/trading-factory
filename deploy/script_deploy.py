from deployer.deploy import deploy_from_config

# SÓ ISSO! Pega tudo do JSON automaticamente
# Usa um arquivo específico
deploy_from_config("combined_strategy_teste.json", strategies_file="entries.py", wait_mode=True)