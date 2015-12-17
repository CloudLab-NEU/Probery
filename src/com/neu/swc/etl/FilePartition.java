package com.neu.swc.etl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class FilePartition {
 
    public static File[] splitDataToSaveFile(int rows, String sourceFilePath, String targetDirectoryPath) {    
          
        File sourceFile = new File(sourceFilePath);  
        File targetFile = new File(targetDirectoryPath);  
        if (!sourceFile.exists() || rows <= 0 || sourceFile.isDirectory()) {  
            return null;  
        }  
        if (targetFile.exists()) {  
            if (!targetFile.isDirectory()) {  
                return null;  
            }  
        } else {  
            targetFile.mkdirs();  
        }  
        try {  
            InputStreamReader in = new InputStreamReader(new FileInputStream(sourceFilePath),"GBK");  
            BufferedReader br=new BufferedReader(in);  
              
            BufferedWriter bw = null;  
            String str = "";  
            String tempData = br.readLine();  
            int i = 1, s = 0;    
            while (tempData != null) {  
               str += tempData + "\r\n";  
                if (i % rows == 0) {  
                    bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(   
                            targetFile.getAbsolutePath() + "/" +  sourceFile.getName() +"_" + (s+1) +".tbl"), "GBK"),1024);   
                      
                    bw.write(str);  
                    bw.close();  
                      
                    str = "";    
                    s += 1;  
                }  
                i++;  
                tempData = br.readLine();  
            }  
            if ((i - 1) % rows != 0) {  
                  
                bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(   
                        targetFile.getAbsolutePath() + "/" +  sourceFile.getName() +"_" + (s+1) +".tbl"), "GBK"),1024);   
                bw.write(str);  
                bw.close();  
                br.close();  
                  
                s += 1;  
            }  
            in.close();  
              
        } catch (Exception e) {  
        }  
        File file = new File(targetDirectoryPath);
        return file.listFiles();
    }  
    
    public static void deleteAll(File file){  
    	if(file.isFile() || file.list().length ==0){
    		file.delete();       
    	}else{    
    		File[] files = file.listFiles();  
    	    for (int i = 0; i < files.length; i++){
    	    	deleteAll(files[i]);  
    	        files[i].delete();      
    	   }  
    	}
    }
}
