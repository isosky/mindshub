import os
import sqlite3
import datetime
from pypinyin import Style, pinyin
from module.dbconn import dbf, dbf_ft, dbf_fb, dbf_fh


def buybook(bookorderform):
    # print(bookorderform)
    if bookorderform['bookid'] != '':
        return updatebook(bookorderform)
    conn = sqlite3.connect(dbf)
    c = conn.cursor()
    content = pinyin(bookorderform['bookname'], style=Style.FIRST_LETTER)
    book_py = ''.join([x[0] for x in content])
    addtime = datetime.datetime.now().strftime('%Y-%m-%d')
    updatetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    c.execute(
        "insert into book (type,sub_type,ztf,book_name,buy_time,location,isbn,addtime,book_py,updatetime) values (?,?,?,?,?,?,?,?,?,?)",
        [bookorderform['booktype'], bookorderform['booksubtype'], bookorderform['ztf'].upper(), bookorderform['bookname'], bookorderform['buytime'],
         bookorderform['location'], bookorderform['ISBN'], addtime, book_py, updatetime])
    conn.commit()
    conn.close()
    return "ok"


def updatebook(bookorderform):
    conn = sqlite3.connect(dbf)
    c = conn.cursor()
    content = pinyin(bookorderform['bookname'], style=Style.FIRST_LETTER)
    book_py = ''.join([x[0] for x in content])
    updatetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    c.execute(
        "update book set type=?,sub_type=?,ztf=?,book_name=?,buy_time=?,location=?,isbn=?,addtime=?,book_py=?,updatetime=? where book_id =? ",
        [bookorderform['booktype'], bookorderform['booksubtype'], bookorderform['ztf'].upper(), bookorderform['bookname'], bookorderform['buytime'],
         bookorderform['location'], bookorderform['ISBN'], bookorderform['buytime'], book_py, updatetime, bookorderform['bookid']])
    conn.commit()
    conn.close()
    return "ok"


def getbook():
    conn = sqlite3.connect(dbf)
    c = conn.cursor()
    temp = c.execute(
        "select book_id,type,sub_type,ztf,book_name,buy_time,location,isbn from book order by book_id desc")
    res = []
    for i in temp:
        res.append({'bookid': i[0], 'booktype': i[1], "booksubtype": i[2], 'ztf': i[3],
                    'bookname': i[4], "buytime": i[5], "location": i[6], "isbn": i[7]})
    conn.commit()
    conn.close()
    return res


def getbookoption():
    conn = sqlite3.connect(dbf)
    c = conn.cursor()
    temp = c.execute("select distinct type from book")
    booktypeoption = []
    for i in temp:
        booktypeoption.append({"value": i[0], 'label': i[0]})
    temp = c.execute("select distinct type,sub_type from book")
    bookalltypeoption = {}
    for i in temp:
        if i[0] not in bookalltypeoption.keys():
            bookalltypeoption[i[0]] = []
        bookalltypeoption[i[0]].append({"value": i[1], 'label': i[1]})
    temp = c.execute("select distinct location from book")
    locationoption = []
    for i in temp:
        locationoption.append({"value": i[0], 'label': i[0]})
    temp = c.execute("select distinct type,sub_type,ztf from book")
    typesuggest = {}
    for i in temp:
        temp_type = i[2].split('-')[0]
        if temp_type not in typesuggest.keys():
            typesuggest[temp_type] = {'type': i[0], "subtype": i[1]}
    conn.commit()
    conn.close()
    return {"booktypeoption": booktypeoption, "bookalltypeoption": bookalltypeoption, "locationoption": locationoption, "typesuggest": typesuggest}


if __name__ == '__main__':
    pass
