import pandas as pd
import json

def apply_etl(df, rules):
    processed_dfs = []
    for index, row in df.iterrows():
        matched = False
        for rule in rules['rules']:
            if str(row['Código']) == str(rule['codigo']):
                divisor = rule['divisor']
                original_qty = row['Quantidade']
                num_parts = int(original_qty // divisor)
                remainder = original_qty % divisor
                for i in range(num_parts):
                    new_row = row.copy()
                    new_row['Quantidade'] = divisor
                    new_row['Lote'] = f"{row['Nf-e']}/{str(i+1).zfill(2)}"
                    processed_dfs.append(new_row)
                if remainder > 0:
                    new_row = row.copy()
                    new_row['Quantidade'] = remainder
                    new_row['Nova Coluna'] = f"{row['Nf-e']}/{str(num_parts+1).zfill(2)}"
                    processed_dfs.append(new_row)
                matched = True
                break
        if not matched:
            new_row = row.copy()
            new_row['Nova Coluna'] = f"{row['Nf-e']}/01"
            processed_dfs.append(new_row)
    return pd.DataFrame(processed_dfs)

# Dados de teste baseados no PDF
data = {
    'Nf-e': [530072],
    'Código': [81600056],
    'Cor Cod': ['00000'],
    'Tam': ['U'],
    'Estampa Cor': ['0000 UNICO'],
    'Quantidade': [1600],
    'Preço': [10]
}
df_test = pd.DataFrame(data)

# Regra baseada no PDF
rules = {
    "rules": [
        {"codigo": "81600056", "divisor": 600}
    ]
}

print("--- Dados Originais ---")
print(df_test)

result = apply_etl(df_test, rules)

print("\n--- Dados Transformados ---")
print(result)

# Verificação
expected_rows = 3 # 600 + 600 + 400
if len(result) == expected_rows:
    print("\n✅ Teste passou: O número de linhas está correto.")
else:
    print(f"\n❌ Teste falhou: Esperava {expected_rows} linhas, mas obteve {len(result)}.")
