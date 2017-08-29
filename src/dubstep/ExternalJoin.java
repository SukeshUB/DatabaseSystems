package dubstep;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

import dubstep.Main.Datatypes;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;


class ExternalJoin{
	static int JoinCount = 1000;
	static int indexCount = 0;
	static int finalJoinCount=0;
	static ArrayList<String[]> list1 = new ArrayList<String[]>();
	static ArrayList<String[]> list2 = new ArrayList<String[]>();
	static String SortMergeJoin(){
		int len = OnDisk.joinPairList.size();
		String t1 = OnDisk.joinPairList.get(len-1).table1;
		String t2 = OnDisk.joinPairList.get(len-1).table2;
		String cName = OnDisk.joinPairList.get(len-1).joinCol;
		String table1_path = OnDisk.dataPath+"sortedTables/"+t1+"_"+cName;
		String table2_path = OnDisk.dataPath+"sortedTables/"+t2+"_"+cName;
		Set<String> listOfCols = new HashSet<String>();
		
		File dir = new File(OnDisk.dataPath+"Join");
		// attempt to create the directory here
		dir.mkdir();
		Map<String,Set<String>> joinedTablesList = new HashMap<String,Set<String>>();
		Set<String> l1 = new LinkedHashSet<String>();
		Set<String> l2 = new LinkedHashSet<String>();
		l1.add(cName);
		l2.add(cName);
		
		joinedTablesList.put(t1,l1);
		joinedTablesList.put(t2,l2);
		
		listOfCols.add(cName);
		if(len == 1){
			DoMerge(table1_path,table2_path,listOfCols,t1,t2,false,true);
			FillJoinSchemaDetails();
			finalJoinCount++;
			return "Join_"+Integer.toString(finalJoinCount-1);
		}
		else{
			DoMerge(table1_path,table2_path,listOfCols,t1,t2,false,false);
		}
		listOfCols.clear();
		ArrayList<Integer> colIndexes = new ArrayList<Integer>();
		ArrayList<Datatypes> colDataTypes = new ArrayList<Datatypes>();
		List<Boolean> colSortType = new ArrayList<Boolean>();
		colSortType.add(true);
		
	/*	for(int i = len-1; i>=0;i--){
			System.out.println("joinPairList1 : "+OnDisk.joinPairList.get(i).table1);
			System.out.println("joinPairList2: "+OnDisk.joinPairList.get(i).table2);
		}
		
		for(Map.Entry<String,Set<String>> m : OnDisk.joinColumns.entrySet()){
			System.err.println("OnDisk Table : "+m.getKey());
			for(String s : m.getValue()){
				System.err.println("OnDisk Columns : "+s);
			}
			
		}
		
		for(Map.Entry<String,OnDisk.colDataType> m : OnDisk.joinTableSchema.entrySet()){
			System.err.println("OnDisk Schema ColName : "+m.getKey()+","+m.getValue().ind+","+m.getValue().dtype);
		}*/
		boolean alreadyJoined = false;
		String prevSortedCol = cName;
		
		//System.err.println("OnDisk prev cName "+cName);
		
		for(int i = len-2; i>=0;i--){
			listOfCols.clear();
			t1 = OnDisk.joinPairList.get(i).table1;
			t2 = OnDisk.joinPairList.get(i).table2;
			cName = OnDisk.joinPairList.get(i).joinCol;
			if(joinedTablesList.containsKey(t1)&&joinedTablesList.containsKey(t2)){
				alreadyJoined = true;
				Set<String> temp1 = joinedTablesList.get(t1);
				temp1.add(cName);
				joinedTablesList.put(t1, temp1);
				Set<String> temp2 = joinedTablesList.get(t2);
				temp2.add(cName);
				joinedTablesList.put(t2,temp2);
				if(joinedTablesList.get(t1).size()>=joinedTablesList.get(t2).size()){
					listOfCols = joinedTablesList.get(t1);
				}
				else{
					listOfCols = joinedTablesList.get(t2);
					t1=t2;
				}
			}
			else if(joinedTablesList.containsKey(t1)){
				Set<String> temp1 = joinedTablesList.get(t1);
				temp1.add(cName);
				joinedTablesList.put(t1, temp1);
				Set<String> temp2 = new HashSet<String>();
				temp2.add(cName);
				joinedTablesList.put(t2, temp2);
				t1 = t2;
				listOfCols.add(cName);
			}
			else if(joinedTablesList.containsKey(t2)){
				Set<String> temp1 = joinedTablesList.get(t2);
				temp1.add(cName);
				joinedTablesList.put(t2, temp1);
				Set<String> temp2 = new HashSet<String>();
				temp2.add(cName);
				joinedTablesList.put(t1, temp2);
				listOfCols.add(cName);
			}
		/*	for(Map.Entry<String,OnDisk.colDataType> m : OnDisk.joinTableSchema.entrySet()){
				System.err.println("OnDisk Schema ColName : "+m.getKey()+","+m.getValue().ind+","+m.getValue().dtype);
			}*/
			table1_path = OnDisk.dataPath+"sortedTables/"+t1+"_"+cName;
		//	System.out.println("Col Name : "+cName);
		//	System.err.println("OnDisk cName "+cName);
			colIndexes.add(OnDisk.joinTableSchema.get(cName).ind);
			colDataTypes.add(OnDisk.joinTableSchema.get(cName).dtype);
			File joinedFile = new File(OnDisk.dataPath+"Join/"+Integer.toString(JoinCount)+".csv");
			if(!joinedFile.exists()){
				return "emptyFile";
			}
			if(prevSortedCol.equals(cName)){
				table2_path = OnDisk.dataPath+"Join/"+Integer.toString(JoinCount)+".csv";
			}
			else{
				table2_path = ExternalSort.DoExternalSorting(OnDisk.dataPath+"Join/"+Integer.toString(JoinCount)+".csv",colIndexes, colDataTypes, colSortType,"",null);
			}
			prevSortedCol = cName;
			colIndexes.clear();
			colDataTypes.clear();
			JoinCount++;
			if(i==0){
				DoMerge(table1_path,table2_path,listOfCols,t1,t2,true,true);
			}
			else{
				DoMerge(table1_path,table2_path,listOfCols,t1,t2,true,false);
			}
			/*for(Map.Entry<String,OnDisk.colDataType> m : OnDisk.joinTableSchema.entrySet()){
				System.out.println("ColName : "+m.getKey()+","+m.getValue().ind+","+m.getValue().dtype);
			}*/
		}
		
	//	JoinCount = 1;
		FillJoinSchemaDetails();
		finalJoinCount++;
		return "Join_"+Integer.toString(finalJoinCount-1);
	}
	
	static void FillJoinSchemaDetails(){
		indexCount = 0;
		int sz = OnDisk.joinTableSchema.size();
		//	InMemory.coldatatypes.clear();
			List<Datatypes> tempList= new ArrayList<Datatypes>(sz);
			
			for (int i = 0; i < sz; i++) {
				tempList.add(Datatypes.UNKNOWN);
				}
		Map<String, TreeMap<Integer, Datatypes>> cmap = new HashMap<String, TreeMap<Integer, Datatypes>>();
		for (Map.Entry<String,OnDisk.colDataType> entry : OnDisk.joinTableSchema.entrySet()) {
			TreeMap<Integer, Datatypes> indexType = new TreeMap<Integer, Datatypes>();
			String key = entry.getKey();
			
		    OnDisk.colDataType value = entry.getValue();
		    indexType.put(value.ind, value.dtype);
		    cmap.put(key,indexType);
		    tempList.set(value.ind,value.dtype);
		 //   System.out.println("Col Name : "+key+", Datatype "+value.dtype+" ,"+" Index "+value.ind);
		}
		 Main.map.put("Join_"+Integer.toString(finalJoinCount),cmap);
		 OnDisk.tableDatatypes.put("Join_"+Integer.toString(finalJoinCount), tempList);
	}
	
	static void DoMerge(String table1_path,String table2_path,Set<String> colName,String table1,
			String table2,boolean joinInitiated, boolean isLastJoin){
		try{
			System.err.println("Merging "+table1+","+table2);
			System.err.println("Paths "+table1_path+","+table2_path);
			BufferedReader br1;
			br1 = new BufferedReader(new FileReader(table1_path));
			List<Integer> ind1 = new ArrayList<Integer>();
			List<Integer> ind2 = new ArrayList<Integer>();
			List<Datatypes> colDatatypes = new ArrayList<Datatypes>();
			
			List<String> list = new ArrayList<String>(colName);
			Collections.reverse(list);
			
			for(String c : list){
				ind1.add(Main.map.get(table1).get(c).firstKey());
				colDatatypes.add(Main.map.get(table1).get(c).firstEntry().getValue());
			}
			
			if(!joinInitiated){
				for(String c : list){
					ind2.add(Main.map.get(table2).get(c).firstKey());
				}
			}
			else{
				for(String c : list){
					ind2.add(OnDisk.joinTableSchema.get(c).ind);
				}
			}
		
			//System.out.println();
			BufferedReader br2;
			br2 = new BufferedReader(new FileReader(table2_path));
			List<Datatypes> dtype1 = OnDisk.tableDatatypes.get(table1);
			List<Datatypes> dtype2 = new ArrayList<Datatypes>();
			if(!joinInitiated){
				dtype2=OnDisk.tableDatatypes.get(table2);
			}
			int counter = 0;	
			List<Integer> index1 = new ArrayList<Integer>();
			List<Integer> index2 = new ArrayList<Integer>();
			
			if(joinInitiated){
				if(OnDisk.joinColumns.containsKey(table1)){
					for(String s : OnDisk.joinColumns.get(table1)){
						if(!OnDisk.joinTableSchema.containsKey(s)){
							int ind = Main.map.get(table1).get(s).firstKey();
							Datatypes dtype = Main.map.get(table1).get(s).get(ind);
							OnDisk.colDataType obj= new OnDisk.colDataType(indexCount,dtype);
							index1.add(ind);
							OnDisk.joinTableSchema.put(s,obj);
							indexCount++;
						}
					}
				}
			}
			else{
				if(OnDisk.joinColumns.containsKey(table1)){
					for(String s : OnDisk.joinColumns.get(table1)){
						int ind = Main.map.get(table1).get(s).firstKey();
						Datatypes dtype = Main.map.get(table1).get(s).get(ind);
						OnDisk.colDataType obj= new OnDisk.colDataType(indexCount,dtype);
						index1.add(ind);
						OnDisk.joinTableSchema.put(s,obj);
						indexCount++;
					}
				}
				if(OnDisk.joinColumns.containsKey(table2)){
					for(String s : OnDisk.joinColumns.get(table2)){
						if(!OnDisk.joinTableSchema.containsKey(s)){
							int ind = Main.map.get(table2).get(s).firstKey();
							Datatypes dtype = Main.map.get(table2).get(s).get(ind);
							OnDisk.colDataType obj= new OnDisk.colDataType(indexCount,dtype);
							index2.add(ind);
							OnDisk.joinTableSchema.put(s,obj);
							indexCount++;
						}
					}
				}	
			}
			
			String line1 = "";
			String line2="";
			boolean read1 = true;
			boolean read2 = true;
			String[] splitLine1 = new String[Main.map.get(table1).size()];
			String[] splitLine2;
		//	String[] savedSplitLine1 = new String[Main.map.get(table1).size()];
		//	String[] savedSplitLine2;
			if(joinInitiated){
				splitLine2 = new String[OnDisk.joinTableSchema.size()];
			//	savedSplitLine2 = new String[OnDisk.joinTableSchema.size()];
			}
			else{
				splitLine2 = new String[Main.map.get(table2).size()];
			//	savedSplitLine2 = new String[OnDisk.joinTableSchema.size()];
			}
			String finalPath="";
			if(isLastJoin){
				finalPath=OnDisk.dataPath+"Join_"+Integer.toString(finalJoinCount)+".csv";
			}
			else{
				finalPath=OnDisk.dataPath+"Join/"+Integer.toString(JoinCount)+".csv";
			}
			File newFile = new File(finalPath);
			if(newFile.exists()){
				newFile.delete();
			}
			newFile.createNewFile();
			FileWriter fwr = new FileWriter(newFile);
		
			BufferedWriter bwr = new BufferedWriter(fwr);
			boolean checkFlag = false;
			int cnt1 = 0;
			int cnt2 = 0;
			boolean read1_2 = true;
			boolean read2_2 = true;
			int counter2=0;
			while(true){
				if(read1){
					if(read1_2){
						line1 = br1.readLine();
						if(line1 == null){
							break;
						}
						splitLine1 = line1.split("\\|");
					}
					
					boolean ret1 = EvaluateWhere(splitLine1,table1,dtype1);
				
					if(!ret1){
						//System.out.println("False for line "+line1);
						//read1 = true;
						read1_2 = true;
						continue;
					}
				}
				if(read2){
					if(read2_2){
						line2 = br2.readLine();
						if(line2 == null){
							break;
						}
						splitLine2 = line2.split("\\|");
					}
					
					if(!joinInitiated){
						boolean ret2 = EvaluateWhere(splitLine2,table2,dtype2);
						if(!ret2){
						//	System.out.println("False for line "+line2);
							read1 = false;
							read2_2 = true;
							continue;
						}
					}
					if(checkFlag){
				//		System.out.println("Second line after where is "+line2);
					}
					
				}
				read1 = true;
				read2 = true;
				read1_2 = true;
				read2_2 = true;
				if(checkFlag){
				//	System.out.println("First line is "+line1);
				//	System.out.println("Second line is "+line2);
				}
				int comp=0;
				int[] tempLine = new int[ind1.size()];
				for(int i = 0 ; i < ind1.size() ; i++){
					String s1 = splitLine1[ind1.get(i)];
					String s2 = splitLine2[ind2.get(i)];
					int n1 = Integer.parseInt(s1);
					int n2 = Integer.parseInt(s2);
					comp = n1-n2;
					tempLine[i]=n1;
					/*if (colDatatypes.get(i) == Datatypes.INT) {
						comp = Integer.parseInt(s1) - Integer.parseInt(s2);
					} else if (colDatatypes.get(i) == Datatypes.DECIMAL) {
						if (Double.parseDouble(s1) - Double.parseDouble(s2) > 0) {
							comp = 1;
						} else if (Double.parseDouble(s1) - Double.parseDouble(s2) < 0) {
							comp = -1;
						}
					} else {
						comp = s1.compareTo(s2);
					}*/
					if(comp!=0){
						break;
					}
				}
				
				if(comp==0){
					list1.add(splitLine1);
					while((line1=br1.readLine())!=null){
						splitLine1 = line1.split("\\|");
						boolean result = false;
						if(ind1.size()==1){
							if(tempLine[0]==Integer.parseInt(splitLine1[ind1.get(0)])){
								result = true;
							}
						}
						else{
							if((tempLine[0]==Integer.parseInt(splitLine1[ind1.get(0)]))
									&& tempLine[1]==Integer.parseInt(splitLine1[ind1.get(1)])){
								result = true;
							}
						}
						
					//	if(AreEqualSplits(splitLine1,tempLine,ind1)){
						if(result){
							boolean ret2 = EvaluateWhere(splitLine1,table1,dtype1);
							if(!ret2){
							//	System.out.println("False for line "+line2);
								continue;
							}
							list1.add(splitLine1);
						}
						else{
							break;
						}
					}
					
					list2.add(splitLine2);
					while((line2=br2.readLine())!=null){
						splitLine2 = line2.split("\\|");
						
						boolean result = false;
						if(ind2.size()==1){
							if(tempLine[0]==Integer.parseInt(splitLine2[ind2.get(0)])){
								result = true;
							}
						}
						else{
							if((tempLine[0]==Integer.parseInt(splitLine2[ind2.get(0)]))
									&& tempLine[1]==Integer.parseInt(splitLine2[ind2.get(1)])){
								result = true;
							}
						}
						
					//	if(AreEqualSplits(splitLine2,tempLine,ind2)){
						if(result){
							if(!joinInitiated){
								boolean ret2 = EvaluateWhere(splitLine2,table2,dtype2);
								if(!ret2){
								//	System.out.println("False for line "+line2);
									continue;
								}
							}
							list2.add(splitLine2);
						}
						else{
							break;
						}
					}
				//	System.out.println("Hello1");
					if(!joinInitiated){
						for(String[] l1:list1){
							
							StringBuilder joinLine= new StringBuilder();
							for(int i1:index1){
								joinLine.append(l1[i1]+"|");
							}
							for(String[] l2:list2){
								StringBuilder joinLine2 = new StringBuilder(joinLine.toString());
								for(int i2:index2){
									joinLine2.append(l2[i2]+"|");
								}
								counter++;
								joinLine2.append("\n");
							//	System.out.println("Hello2");
								bwr.write(joinLine2.toString());
							
							}
							
						}
					}
					else{
						for(String[] l1:list1){
							StringBuilder joinLine2= new StringBuilder();
							for(int i1:index1){
								joinLine2.append(l1[i1]+"|");
							}
							for(String[] l2:list2){
								StringBuilder joinLine= new StringBuilder();
								for(String s : l2){
									joinLine.append(s+"|");
								}
								joinLine.append(joinLine2.toString());
								counter++;
								joinLine.append("\n");
								bwr.write(joinLine.toString());
							}
						}
					}
					
					read1 = true;
					read2 = true;
					read1_2 = false;
					read2_2 = false;
					list1.clear();
					list2.clear();
					if(line1 == null || line2 == null){
						break;
					}
				}
				else if(comp>0){
					read1 = false;
					read1_2 = false;
				}
				else{
					read2 = false;
					read2_2 = false;
				}
			}
		//	System.out.println("Cnt1 "+counter);
		//	System.out.println("Cnt2 "+counter2);
			br1.close();
			br2.close();
			bwr.close();
		}
		catch(Exception e){
			System.out.println("Exception in DoMerge "+e);
		}
		
		
	}
	
	static boolean AreEqualSplits(String[] line1,String[] line2,List<Integer> index){
		int k = 0;
		for(int i:index){
			if(!line1[i].equals(line2[k])){
				return false;
			}
			k++;
		}
		return true;
	}
	
	static boolean EvaluateWhere(String[] line,String tableName,List<Datatypes> dtype){
		try{
			if(!OnDisk.tableWhere.containsKey(tableName)){
				return true;
			}
			Main.vals = line;
			
			Main.valslist = new ArrayList<PrimitiveValue>();
			for (int i = 0; i < Main.vals.length; i++) {
				if (dtype.get(i) == Datatypes.INT) {
					Main.valslist.add(new LongValue(Main.vals[i]));
				} else if (dtype.get(i) == Datatypes.DECIMAL) {
					Main.valslist.add(new DoubleValue(Main.vals[i]));
				} else if (dtype.get(i) == Datatypes.CHAR) {
					Main.valslist.add(new StringValue(Main.vals[i]));
				} else if (dtype.get(i) == Datatypes.STRING) {
					Main.valslist.add(new StringValue(Main.vals[i]));
				} else if (dtype.get(i) == Datatypes.DATE) {
					Main.valslist.add(new DateValue(Main.vals[i]));
				}
				else if (dtype.get(i) == Datatypes.VARCHAR) {
					Main.valslist.add(new StringValue(Main.vals[i]));
				}
			}
			Main.tableName = tableName;
			for(Expression expr: OnDisk.tableWhere.get(tableName)){
				if(!Main.evaluateWhere(Main.valslist, expr)){
					return false;
				}
			}
			return true;
		}
		catch(Exception e){
			System.out.println("Exception in Evaluate Where "+e);
			return false;
		}
	
		
	}
}