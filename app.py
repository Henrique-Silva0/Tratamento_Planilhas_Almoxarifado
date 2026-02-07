import streamlit as st
import pandas as pd
import json
import io
import os
import openpyxl as op
import xlsxwriter
import zipfile
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.styles import Border

# Configuração da página
st.set_page_config(page_title="Ferramenta de ETL Customizada", layout="wide")

#Página SáBooor FrontEnd
st.markdown("""
<style>

/* Esconde o label do multiselect */
.stMultiSelect label {
    display: none;
}

/* Texto das opções do dropdown */
div[data-baseweb="menu"] span {
    font-size: 20px !important;
    color: white;
}

/* Fundo principal */
section[data-testid="stAppViewContainer"] {
    background-color: #badea6;
}

/* Multiselect */
div[data-testid="stMultiSelect"] > div {
    background-color: #388D2B;
    border: 2px solid #388D2B;
    border-radius: 18px;
    padding: 6px;
}

/* Hover */
div[data-testid="stMultiSelect"]:hover > div {
    background-color: #2f7a24;
}

/* Tags selecionadas do multiselect */
div[data-testid="stMultiSelect"] span[data-baseweb="tag"] {
    background-color: #28991f !important;  /* VERDE */
    color: white !important;
    border-radius: 12px;
    padding: 10px 28px;
    font-weight: 500;
}

/* Botões */
.stButton > button,
.stDownloadButton > button {
    width: 100%;
    height: 3em;
    border-radius: 6px;
    background-color: #28991f;
    color: white;
}

/* Botões da sidebar */
div[data-testid="stSidebar"] .stButton > button {
    background-color: #dc3545;
    color: white;
    border-radius: 6px;
    border: none;
}

div[data-testid="stSidebar"] .stButton > button:hover {
    background-color: #a71d2a;
}

</style>
""", unsafe_allow_html=True)


def load_rules():
    if os.path.exists('rules.json'):
        with open('rules.json', 'r') as f:
            return json.load(f)
    return {"rules": []}

def save_rules(rules):
    with open('rules.json', 'w') as f:
        json.dump(rules, f, indent=4)

def processo_etl(df, rules):
    # 1. Identificar Colunas (Já feito pelo pandas no upload)
    
    # 2. Excluir Colunas Desnecessárias (Simulado por seleção do usuário na UI)
    
    # 3. Dividir tabela com base em uma coluna (Nf-e)
    # No exemplo do PDF, parece que ele quer quebrar linhas baseado em regras de quantidade
    
    processos_etl = []

    for index, row in df.iterrows():
        matched = False
        for rule in rules['rules']:
            if str(row['Código']) == str(rule['codigo']):
                divisor = rule['divisor']
                original_qty = row['Quantidade']

                # Lógica de divisão
                num_parts = int(original_qty // divisor)
                remainder = original_qty % divisor

                linhas_temp = []
                for i in range(num_parts):
                    new_row = row.copy()
                    new_row['Quantidade'] = divisor
                    new_row['LOTE'] = f"{row['Nf-e']}/{str(i+1).zfill(2)}"
                    linhas_temp.append(new_row)

                if remainder > 0:
                    new_row = row.copy()
                    new_row['Quantidade'] = remainder
                    linhas_temp.append(new_row)

                # Ordena para que o menor valor de Quantidade fique primeiro
                linhas_ordenadas = sorted(linhas_temp, key=lambda x: x['Quantidade'])
                processos_etl.extend(linhas_ordenadas)

                for i, new_row in enumerate(linhas_ordenadas, start=1):
                    new_row['LOTE'] = f"{row['Nf-e']}/{str(i).zfill(2)}"

                matched = True
                break

        if not matched:
            new_row = row.copy()
            new_row['LOTE'] = f"{row['Nf-e']}/01"
            processos_etl.append(new_row)

    return pd.DataFrame(processos_etl)


def main():
    st.title("🚀 Tratamento de Planilha Automatizado")
    st.markdown("#### O processo pode se mostrar difícil; portanto, vamos buscar maneiras de torná-lo mais simples.")

    # Sidebar para Gerenciamento de Regras
    st.divider()
    st.sidebar.image(os.path.join("Imagens", "Logo_Vectra0.png"), width=300)
    st.sidebar.divider()
    st.sidebar.header("⚙️ Configurações de Regras")
    rules = load_rules()

    with st.sidebar.expander("Adicionar Nova Regra"):
        new_cod = st.text_input("Código do Produto")
        new_div = st.number_input("Divisor (Quantidade)", min_value=1, value=600)
        if st.button("Salvar Regra"):
            # Verifica se o código já existe
            regra_existe = any(
                r["codigo"] == new_cod
                for r in rules["rules"]
            )

            if regra_existe:
                st.warning("⚠️ Essa regra já existe")
            else:
                rules["rules"].append({
                    "codigo": new_cod,
                    "divisor": new_div
                })
                save_rules(rules)
                st.success("✅ Regra adicionada!")
                st.rerun()

    if rules['rules']:
        st.sidebar.subheader("Regras Atuais")
        for i, r in enumerate(rules['rules']):
            st.sidebar.text(f"Cód: {r['codigo']} -> Div: {r['divisor']}")
            
            if st.sidebar.button(f"Remover {i}", key=f"del_{i}"):
                rules['rules'].pop(i)
                save_rules(rules)
                st.rerun()


    # Upload de Arquivo
    st.markdown("#### Importe o Romaneio 105 - J")
    uploaded_file = st.file_uploader("Escolha um arquivo", type=['xlsx'])


    if uploaded_file is not None:
        try:
            df = pd.read_excel(uploaded_file, dtype={'Código': str,'Cor Cod': str,'Tam': str,'Estampa': str})

            st.subheader("Dados Originais")
            st.dataframe(df.reset_index(drop=True), use_container_width=True)


            # Seleção de Colunas para Manter
            todas_colunas = df.columns.tolist()
            colunas_padrao = ['Nf-e','Código','Cor Cod','Tam','Estampa','Quantidade','Preço'] 
            st.markdown("#### Selecione as colunas que deseja manter")
            cols_to_keep = st.multiselect("Selecione as colunas que deseja manter", todas_colunas, default=colunas_padrao)
            
            col1, col2, col3 = st.columns([1, 1, 1])

            if st.button("Executar Transformação ETL"):
                df_filtered = df[cols_to_keep]

                # Aplicar Lógica de ETL
                with st.spinner('Processando dados...'):
                    result_df = processo_etl(df_filtered, rules)
                    result_df = result_df.rename(columns={"Código": "COD_PRODUTO",
                                                        "Cor Cod":"COD_COR",
                                                        "Tam": "TAMANHO",
                                                        "Estampa": "COD_ESTAMPA",
                                                        "Quantidade": "QUANTIDADE",
                                                        "Preço": "PRECO"})



                st.subheader("Dados Transformados") 

                nf_values = result_df['Nf-e'].unique()

                # --- Botões individuais por NF-e ---
                for nf in nf_values:
                    df_nf = result_df[result_df['Nf-e'] == nf].copy()
                    df_nf = df_nf.drop('Nf-e', axis=1)

                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df_nf.to_excel(writer, index=False, sheet_name='Resultado_ETL')
    
                        ws = writer.book['Resultado_ETL']
                        for cell in ws[1]: 
                        # primeira linha = cabeçalhos 
                            cell.font = Font(bold=False) # sem negrito 
                            cell.alignment = Alignment(horizontal="left") # alinhado à esquerda
                            cell.border = Border() # remove qualquer borda aplicada


                    nome_arquivo = f"Resultado_ETL_NF_{nf}_Rom_.xlsx"

                    with col1:
                        st.download_button(
                            label=f"📥 Baixar Resultado NF-e {nf}",
                            data=output.getvalue(),
                            file_name=nome_arquivo,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                st.dataframe(df_nf,hide_index=True)
                # --- Botão único para baixar todos em ZIP ---
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, "w") as zf:
                    for nf in nf_values:
                        df_nf = result_df[result_df['Nf-e'] == nf].copy()
                        df_nf = df_nf.drop('Nf-e', axis=1)

                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            df_nf.to_excel(writer, index=False, sheet_name='Resultado_ETL')

                            ws = writer.book['Resultado_ETL']
                            for cell in ws[1]: 
                            # primeira linha = cabeçalhos 
                                cell.font = Font(bold=False) # sem negrito 
                                cell.alignment = Alignment(horizontal="left") # alinhado à esquerda
                                cell.border = Border() # remove qualquer borda aplicada

                        nome_arquivo = f"Resultado_ETL_NF_{nf}_Rom_.xlsx"
                        zf.writestr(nome_arquivo, output.getvalue())

                with col2:
                    st.download_button(
                        label="📦 Baixar Todos os Arquivos (ZIP)",
                        data=zip_buffer.getvalue(),
                        file_name="Resultados_ETL.zip",
                        mime="application/zip"
                    )
                    
        except Exception as e:
            st.error(f"Erro ao processar arquivo: {e}")

if __name__ == "__main__":
    main()
