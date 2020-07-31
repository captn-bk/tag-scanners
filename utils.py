





# function to return the RSI tag
def apply_rsi_tag(row):
    if row['rsi'] < 30:
        val = '*OVERSOLD*'
    elif row['rsi'] > 70:
        val = '*OVERBOUGHT*'
    else:
        val = ''
    return val