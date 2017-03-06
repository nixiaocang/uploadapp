#!/usr/bin/env python  
#coding=utf-8  

import wx
from login import LoginHelper
from upload import UploadHelper
from add import Addhelper
class MyFrame(wx.Frame):
    def __init__(self, parent=None, title=u'BDP文件传输工具 v0.0.1'):
        wx.Frame.__init__(self, parent, -1, title=title)
        self.panel = wx.Panel(self, style=wx.TAB_TRAVERSAL | wx.CLIP_CHILDREN | wx.FULL_REPAINT_ON_RESIZE)

        #增加一些控件:用户名密码部分，并使用GridBagSizer来管理这些控件  
        self.label1=wx.StaticText(self.panel,-1,label=u'企业域：')
        self.label2=wx.StaticText(self.panel,-1,label=u'用户名：')
        self.label3=wx.StaticText(self.panel,-1,label=u'密     码：')
        self.domainText=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.userText=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.passText=wx.TextCtrl(self.panel,-1,size=(200,25), style=wx.TE_PASSWORD)
        self.loginBtn=wx.Button(self.panel,1,label=u'登录')

        self.gbsizer1=wx.GridBagSizer(hgap=10, vgap=10)
        self.gbsizer1.Add(self.label1,pos=(2,5),span=(1,1),flag=wx.ALIGN_RIGHT|wx.ALIGN_CENTRE_VERTICAL)
        self.gbsizer1.Add(self.domainText,pos=(2,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer1.Add(self.label2,pos=(3,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer1.Add(self.userText,pos=(3,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer1.Add(self.label3,pos=(4,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer1.Add(self.passText,pos=(4,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer1.Add(self.loginBtn,pos=(5,6),span=(1,1),flag=wx.EXPAND)
        #增加一些控件:最下方的按钮，并使用水平方向的BoxSizer来管理这些控件  
        #给"服务器设置"按钮绑定事件处理器  
        self.loginBtn.Bind(wx.EVT_BUTTON,self.OnTouch)

        # 定义第二面板
        self.dlabel=wx.StaticText(self.panel,-1,label=u'数据源名称：')
        self.dtext=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.tlabel=wx.StaticText(self.panel,-1,label=u'表名称：')
        self.ttext=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.flabel=wx.StaticText(self.panel,-1,label=u'文件路径：')
        self.ftext=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.slabel=wx.StaticText(self.panel,-1,label=u'分隔符：')
        self.stext=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.nlabel=wx.StaticText(self.panel,-1,label=u'空值：')
        self.ntext=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.putBtn=wx.Button(self.panel,1,label=u'上传')
        self.rlabel=wx.StaticText(self.panel,-1,label=u'处理结果:')
        self.rtext=wx.TextCtrl(self.panel,-1,size=(200,100), style=wx.TE_MULTILINE)

        self.gbsizer2=wx.GridBagSizer(hgap=10, vgap=10)
        self.gbsizer2.Add(self.dlabel,pos=(1,5),span=(1,1),flag=wx.ALIGN_RIGHT|wx.ALIGN_CENTRE_VERTICAL)
        self.gbsizer2.Add(self.dtext,pos=(1,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer2.Add(self.tlabel,pos=(2,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer2.Add(self.ttext,pos=(2,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer2.Add(self.flabel,pos=(3,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer2.Add(self.ftext,pos=(3,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer2.Add(self.slabel,pos=(4,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer2.Add(self.stext,pos=(4,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer2.Add(self.nlabel,pos=(5,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer2.Add(self.ntext,pos=(5,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer2.Add(self.putBtn,pos=(6,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer2.Add(self.rlabel,pos=(8,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer2.Add(self.rtext,pos=(8,6),span=(1,1),flag=wx.EXPAND)
        self.putBtn.Bind(wx.EVT_BUTTON,self.OnPut)

        self.gbsizer3=wx.GridBagSizer(hgap=10, vgap=10)
        self.retryBtn=wx.Button(self.panel,1,label=u'重试')
        self.gbsizer3.Add(self.retryBtn,pos=(1,10),span=(1,1),flag=wx.EXPAND)
        self.retryBtn.Bind(wx.EVT_BUTTON,self.RetyPut)

        self.gbsizer4=wx.GridBagSizer(hgap=10, vgap=10)
        self.createBtn=wx.Button(self.panel,1,label=u'新建')
        self.appendBtn=wx.Button(self.panel,1,label=u'追加')
        self.gbsizer4.Add(self.createBtn,pos=(1,10),span=(1,1),flag=wx.EXPAND)
        self.gbsizer4.Add(self.appendBtn,pos=(2,10),span=(1,1),flag=wx.EXPAND)
        self.createBtn.Bind(wx.EVT_BUTTON,self.create)
        self.appendBtn.Bind(wx.EVT_BUTTON,self.append)

        # 定义追加面板
        self.dsid_label=wx.StaticText(self.panel,-1,label=u'数据源id：')
        self.dsid_text=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.tbid_label=wx.StaticText(self.panel,-1,label=u'工作表id：')
        self.tbid_text=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.adpath_label=wx.StaticText(self.panel,-1,label=u'文件路径：')
        self.adpath_text=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.ads_label=wx.StaticText(self.panel,-1,label=u'分隔符：')
        self.ads_text=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.adn_label=wx.StaticText(self.panel,-1,label=u'空值：')
        self.adn_text=wx.TextCtrl(self.panel,-1,size=(200,25))
        self.addBtn=wx.Button(self.panel,1,label=u'上传')
        self.arlabel=wx.StaticText(self.panel,-1,label=u'处理结果:')
        self.artext=wx.TextCtrl(self.panel,-1,size=(200,100), style=wx.TE_MULTILINE)

        self.gbsizer5=wx.GridBagSizer(hgap=10, vgap=10)
        self.gbsizer5.Add(self.dsid_label,pos=(1,5),span=(1,1),flag=wx.ALIGN_RIGHT|wx.ALIGN_CENTRE_VERTICAL)
        self.gbsizer5.Add(self.dsid_text,pos=(1,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer5.Add(self.tbid_label,pos=(2,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer5.Add(self.tbid_text,pos=(2,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer5.Add(self.adpath_label,pos=(3,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer5.Add(self.adpath_text,pos=(3,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer5.Add(self.ads_label,pos=(4,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer5.Add(self.ads_text,pos=(4,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer5.Add(self.adn_label,pos=(5,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer5.Add(self.adn_text,pos=(5,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer5.Add(self.addBtn,pos=(6,6),span=(1,1),flag=wx.EXPAND)
        self.gbsizer5.Add(self.arlabel,pos=(8,5),span=(1,1),flag=wx.EXPAND)
        self.gbsizer5.Add(self.artext,pos=(8,6),span=(1,1),flag=wx.EXPAND)
        self.addBtn.Bind(wx.EVT_BUTTON,self.adata)


        #增加BoxSizer,管理用户名密码部分的gbsizer1，  
        #服务器设置部分的sbsizer，以及最下方的bsizer  
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(self.gbsizer1, 0, wx.EXPAND, 20)
        self.sizer.Add(self.gbsizer2, 0, wx.EXPAND, 20)
        self.sizer.Add(self.gbsizer5, 0, wx.EXPAND, 20)
        self.sizer.Add(self.gbsizer3, 0, wx.EXPAND, 20)
        self.sizer.Add(self.gbsizer4, 0, wx.EXPAND, 20)
        self.sizer.Hide(self.gbsizer2)
        self.sizer.Hide(self.gbsizer3)
        self.sizer.Hide(self.gbsizer4)
        self.sizer.Hide(self.gbsizer5)
        self.isShown = False    #用这个变量指示当前是否已将控件隐藏  
        self.SetClientSize((600,480))    #更改面板尺寸 d

        self.panel.SetSizerAndFit(self.sizer)
        self.sizer.SetSizeHints(self.panel)

    def create(self, event):
        self.sizer.Hide(self.gbsizer4)
        self.sizer.Show(self.gbsizer2)
        self.sizer.Layout()
        return True

    def append(self, event):
        self.sizer.Hide(self.gbsizer4)
        self.sizer.Remove(self.gbsizer2)
        self.sizer.Show(self.gbsizer5)
        self.sizer.Layout()
        return True

    def OnTouch(self, event):
        domain = self.domainText.GetValue()
        username = self.userText.GetValue()
        password = self.passText.GetValue()
        if (str(domain)=='' or str(password)=='' or str(username)==''):
            wx.MessageBox("请输入完整的参数", caption="Message", style=wx.OK)
            return True
        try:
            res = LoginHelper(domain, username, password).login()
        except Exception, e:
            dlg = wx.MessageDialog(None, "%s" % e.message, u"错误信息", wx.YES_NO | wx.ICON_QUESTION)
            if dlg.ShowModal() == wx.ID_YES:
                dlg.Destroy()
            return True
        self.sizer.Hide(self.gbsizer1)
        self.sizer.Show(self.gbsizer4)
        self.sizer.Layout()    #关键所在，强制sizer重新计算并布局sizer中的控件  
        self.user_id = res
        return True

    def OnPut(self, event):
        ds_name = self.dtext.GetValue().encode('utf-8')
        tbname = self.ttext.GetValue().encode('utf-8')
        path = self.ftext.GetValue().encode('utf-8')
        separator = self.stext.GetValue().encode('utf-8')
        null_holder = self.ntext.GetValue().encode('utf-8')
        if (str(ds_name)=='' or str(tbname)=='' or str(path)=='' or str(separator)=='' or str(null_holder)==''):
            wx.MessageBox("请输入完整的参数", caption="Message", style=wx.OK)
            return True
        if path.endswith('/'):
            path = path[:-1]
        status, res = UploadHelper().do_action(self.user_id, path, tbname, ds_name, separator, null_holder)
        self.rtext.SetValue(res)
        if status != 0:
            self.sizer.Show(self.gbsizer3)
            self.sizer.Layout()
        return True
    def adata(self, event):
        ds_id = self.dsid_text.GetValue().encode('utf-8')
        tb_id = self.tbid_text.GetValue().encode('utf-8')
        path = self.adpath_text.GetValue().encode('utf-8')
        separator = self.ads_text.GetValue().encode('utf-8')
        null_holder = self.adn_text.GetValue().encode('utf-8')
        if (str(ds_id)=='' or str(tb_id)=='' or str(path)=='' or str(separator)=='' or str(null_holder)==''):
            wx.MessageBox("请输入完整的参数", caption="Message", style=wx.OK)
            return True
        if path.endswith('/'):
            path = path[:-1]
        status, res = Addhelper().do_action(path, separator, null_holder, ds_id, tb_id , self.user_id)
        self.artext.SetValue(res)
        if status != 0:
            self.sizer.Show(self.gbsizer3)
            self.sizer.Layout()
        return True

    def RetyPut(self, event):
        status, res = UploadHelper().retry(path)
        self.rtext.SetValue(res)
        self.artext.SetValue(res)
        if status == 0:
            self.sizer.Hide(self.gbsizer3)
            self.sizer.Layout()
        return True

if __name__ == "__main__":
    app = wx.PySimpleApp()
    frame = MyFrame(None)
    frame.Show(True)
    app.MainLoop()

