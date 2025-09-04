import a_funciones as fun

Almacenes = fun.extraer_datos()

Almacenes = fun.limpiar_espacios(Almacenes, 'NombreVendedorDestino')

Almacenes_clean,POS=fun.limp_trans(Almacenes)

df = fun.rank(Almacenes_clean)

df_total, df_final=fun.tran2(df, POS)

pivot_df=fun.resumen_pivot(df_total)

pivot_df.to_excel('RESULTADOS\\datos.xlsx')