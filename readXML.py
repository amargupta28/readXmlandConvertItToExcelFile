import pandas as pd
import xml.etree.ElementTree as et
from datetime import date
import pyodbc


def nodeIterator(nodeItem):
    """ This method is to find out child elements
     and fetch the vlaue and put it in dict and return it
    
    """
    try:
        temp={}
        tempItem=[]
        for num,item in enumerate(nodeItem,start =1):
            if(len(item)):
                temp[(item.tag).split('}')[1]+"_"+str(num)] = nodeIterator(item)
            else:
                temp[(item.tag).split('}')[1]] = item.text if item.text is not None else " "

        tempItem.append(temp)
        return tempItem


    except Exception as err:
        print("node iterator block error: ",err)



def readxmlFile(fileName):
    """
    This method basically read XML file and iterate over it
    """
    try:
        xRead =et.parse(fileName)
        rootEle = xRead.getroot()
        rootElements={}
        for node in rootEle:
            if len(node) != 0 :
                item = nodeIterator(node)
                rootElements[(node.tag).split('}')[1]] = item

            else :
                rootElements[(node.tag).split('}')[1]]= node.text if node.text is not None else " "

        return rootElements

    except Exception as error:
        print("read File format error: ",error)



def dataFormatting(dataList):

    '''
    Formatting data as a single line of order in a single row
    '''

    rowsList=[]
    temprowList={}
    

    try:

        """
        first identifying no. of items ordered
        """

        if len(dataList["ORDERLINES"][0].keys()) != 0 :
            for item in dataList["ORDERLINES"][0].keys():     
                temprowList={}          
                for i in dataList["ORDERLINES"][0][item][0].keys():
                    # print(dataList["ORDERLINES"][0][item][0][i])
                    temprowList[i] = dataList["ORDERLINES"][0][item][0][i] 
                rowsList.append(temprowList)

        # print(rowsList)


        
        for item in dataList.keys():

            if type(dataList[item]) == str or  type(dataList[item]) == None :
                for i in range(0,len(rowsList)):
                    rowsList[i][item] = dataList[item] 
            elif item != "ORDERLINES":
                for j in range(0,len(rowsList)):
                    for k in dataList[item][0].keys():
                        rowsList[j][k] = dataList[item][0][k] 


        return rowsList
    except Exception as err:
        print("Formatting Error BLock : ",err)



def dataToDatabase(df):

    """
    To Push data into SQL Server

    """
    try:

        server = 'yourservername' 
        database = 'yourDatabaseName'
        username = 'username' 
        password = 'yourpassword' 
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO xyz ({{columnName}}) values(?,?,?)",
                {{row.columnName)}}
            cnxn.commit()
            cursor.close()

    except Exception as err:
        print("Database block error: " + err)



if __name__ == "__main__" :

    ############################# Need USER INPUT ##############################

    """
    change the file name and make sure file is in same folder as the code is in

    """
    fileName= "sampleTest.xml"
    dataParsed = readxmlFile(fileName) 

    print(dataParsed)

    #########################################################################


    finalCsvDdata=dataFormatting(dataParsed)

    # print(dataParsed)

    df = pd.DataFrame(data=finalCsvDdata)
    df.to_excel(fileName.split(".")[0]+str(date.today())+".xlsx",index =False)

    # dataToDatabase(df)



