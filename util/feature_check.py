import re

feature_list = ['humor', 'teetee', 'faq', 'lewd', 'clip', 'fail', 'hic', 'inaff', 'guh', 'superchat', 'tmt', 'bottomleft']

def has_humor(msg, streamer):
    #humor_list = ["草", "kusa", "grass", "茶葉", "_fbkcha", "_lol", "lmao", "lmfao", "haha", "🤣", "😆", "jaja", "笑",
    #                "xd", "wkwk", "ｗ"]
    
    humor_list = ["草", "茶葉", "_fbkcha", "_lol", "lmao", "lmfao", "haha", "🤣", "😆", "jaja", "笑",
                    "xd", "wkwk", "ｗ"]
                    
    # check if message has japanese and ends with ascii 'w'
    jp_regex = "[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uff66-\uff9f]"
    has_jp = re.search(jp_regex, msg)
    has_w_end = has_jp and msg.endswith("w")
    
    # ignore kusa sticker where it's used indiscriminately
    kusa_sticker_ignore_list = ["Coco"]
    kusa_sticker_ignore = streamer in kusa_sticker_ignore_list and "_kusa" in msg
    
    # exact string check for "lol"
    has_lol = re.search(r"\blol\b", msg)
    
    return (any(substring in msg for substring in humor_list) or has_w_end or has_lol) and not kusa_sticker_ignore
        
def has_teetee(msg):
    teetee_list = ["てぇてぇ", ":_tee::_tee:", "tee tee", "teetee", "tete"]
    
    return any(substring in msg for substring in teetee_list)
    
def has_faq(msg):
    return "faq" in msg
    
def has_lewd(msg):
    return "lewd" in msg
    
def has_clip(msg):
    return "clip" in msg
    
def has_fail(msg):
    has_f = re.search(r"\bf\b", msg)
    has_rip = re.search(r"\brip\b", msg)
    has_fail = re.search(r"\bfail\b", msg)
    return has_f or has_rip or has_fail

def has_hic(msg):
    has_hic1 = re.search(r"\bhic\b", msg)
    has_hic2 = re.search(r"\bh i c\b", msg)
    has_hic3 = re.search(r":_hic1::_hic2:_hic3:", msg)
    no_and_tear = "and tear" not in msg
    
    return (has_hic1 or has_hic2 or has_hic3) and no_and_tear
    
def has_inaff(msg):
    return "inaff" in msg
    
def has_guh(msg):
    has_guh1 = re.search(r"\bguh\b", msg)
    has_guh2 = re.search(r":_moriguh:", msg)
    return has_guh1 or has_guh2
    
def has_tmt(msg):
    has_tmt = re.search(r"\btmt\b", msg)
    has_tmd = re.search(r"\btmd\b", msg)
    return has_tmt or has_tmd
    
def has_bottomleft(msg):
    return ":_bottomleft:" in msg