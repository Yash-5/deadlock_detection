import java.io.*;  
import java.net.*;
import java.util.*;
import java.math.*;
import java.text.*;
import java.util.concurrent.ThreadLocalRandom;

public class ChandyANDModel {
	public enum msgType {
		PROBE, DEADLOCK
	}

	public enum processType {
		ACTIVE, WAITING, DEADLOCKED
	}

	static Integer myId = 0;
	static Integer numProcess = 0;
	static ArrayList<String> ipList = new ArrayList<String>();
	static ArrayList<ArrayList<Integer>> adjList = new ArrayList<ArrayList<Integer>>();
	static ArrayList<ArrayList<Integer>> wfgList = new ArrayList<ArrayList<Integer>>();
	static ArrayList<ArrayList<Integer>> routing = new ArrayList<ArrayList<Integer>>();
	static HashSet<Integer> dependants = new HashSet<>();
	static HashMap<Integer, Socket> socketMap = new HashMap<Integer, Socket>();
	static HashSet<Integer> reqSent = new HashSet<Integer>();
	static processType myType = processType.ACTIVE;
	static Boolean probeFlag = false;
	
	static Integer getHash(Integer x, Integer y) {
		return numProcess * Math.min(x, y) + Math.max(x, y) + 2000;
	}

	public static Boolean bfs(Integer curr) {
		Integer cntVisited = 0;
		ArrayList<Boolean> visited = new ArrayList<Boolean>();
		for(int i = 0; i < numProcess; i++) {
			visited.add(false);
		}
		routing.get(curr).set(curr, -1);
		visited.set(curr, true);
		cntVisited++;
		Queue<Integer> nodes = new LinkedList<Integer>();
		nodes.add(-1);
		nodes.add(curr);
		while(!nodes.isEmpty()) {
			Integer par = nodes.poll();
			Integer x = nodes.poll();
			for(Integer neigh: adjList.get(x)) {
				if(!visited.get(neigh)) {
					if(par == -1) {
						nodes.add(curr);
						nodes.add(neigh);
						routing.get(curr).set(neigh, neigh);
					} else if(par == curr) {
						nodes.add(x);
						nodes.add(neigh);
						routing.get(curr).set(neigh, x);
					} else {
						nodes.add(par);
						nodes.add(neigh);
						routing.get(curr).set(neigh, par);
					}
					visited.set(neigh, true);
					cntVisited++;
				}
			}
		}
		return (cntVisited == numProcess);
	}

	static String makeMsg(Integer orig, Integer sender, Integer target, Integer flag) {
		return orig.toString() + " " + sender.toString() + " " + target.toString() + " " + flag.toString();
	}

	static void sendDeadlockMsg(ArrayList<Integer> routing, Integer dest) throws IOException {
		String outMsg = makeMsg(myId, myId, dest, msgType.DEADLOCK.ordinal());
		DataOutputStream dout = new DataOutputStream(socketMap.get(routing.get(dest)).getOutputStream());
		dout.writeUTF(outMsg);
		dout.flush();
	}

	static void handleDeadlock(ArrayList<Integer> routing) {
		if (myType == processType.DEADLOCKED) {
			return;
		}
		System.out.println("Deadlock detected");
		myType = processType.DEADLOCKED;
		for(Integer x: dependants) {
			try {
				sendDeadlockMsg(routing, x);
				System.out.println((myId + 1) + " sent a DEADLOCK message to " + (x + 1));
			} catch(Exception e) {
				continue;
			}
		}
	}

	static void dowork(ArrayList<Integer> routing) throws Exception {
		if(wfgList.get(myId).contains(myId)) {
			handleDeadlock(routing);
		}
		if (probeFlag) {
			for(Integer x: wfgList.get(myId)) {
				Integer intermediate = routing.get(x);
				String probeMsg = makeMsg(myId, myId, x, msgType.PROBE.ordinal());
				DataOutputStream dout = new DataOutputStream(socketMap.get(intermediate).getOutputStream());
				dout.writeUTF(probeMsg);
				System.out.println((myId + 1) + " sent a PROBE message to " + (x + 1));
				dout.flush();
			}
		}
		while(true) {
			for(Integer x: adjList.get(myId)) {
				String msg;
				try {
					DataInputStream dis = new DataInputStream(socketMap.get(x).getInputStream());
					msg = (String)dis.readUTF();
				} catch(Exception e) {	//No message
					continue;
				}
				String[] inMsg = msg.split(" ");
				Integer orig = Integer.parseInt(inMsg[0]);
				Integer sender = Integer.parseInt(inMsg[1]);
				Integer target = Integer.parseInt(inMsg[2]);
				msgType type = msgType.values()[Integer.parseInt(inMsg[3])];
				if (target != myId) {	//I'm not the target, just forward
					String outMsg = makeMsg(orig, sender, target, type.ordinal());
					DataOutputStream dout = new DataOutputStream(socketMap.get(routing.get(target)).getOutputStream());
					dout.writeUTF(outMsg);
					dout.flush();
					continue;
				}
				if (type == msgType.PROBE) {	//It's a probe message
					System.out.println((myId + 1) + " received a PROBE message from " + (sender + 1));
					if (orig == myId) {	//I am the origin, so there's a deadlock	
						handleDeadlock(routing);
					} else if (myType == processType.DEADLOCKED) {
						sendDeadlockMsg(routing, sender);
						System.out.println((myId + 1) + " sent a DEADLOCK message to " + (sender + 1));
					} else if (myType == processType.WAITING) {	//I got a probe message, probe all those that I depend upon if not already done
						if (!reqSent.contains(orig)) { //First one	
							for (Integer wf: wfgList.get(myId)) {
								Integer intermediate = routing.get(wf);
								String probeMsg = makeMsg(orig, myId, wf, msgType.PROBE.ordinal());
								DataOutputStream dout = new DataOutputStream(socketMap.get(intermediate).getOutputStream());
								dout.writeUTF(probeMsg);
								dout.flush();
								System.out.println((myId + 1) + " sent a PROBE message to " + (wf + 1));
							}
						}
						dependants.add(orig);
						dependants.add(sender);
						reqSent.add(orig);
					}
				} else { //Flag is 1 now, it's a deadlock message
					System.out.println((myId + 1) + " received a DEADLOCK message from " + (sender + 1));
					handleDeadlock(routing);
				}
			}
		}
	}

	static ArrayList<ArrayList<Integer>> getList(Boolean isDirected, BufferedReader inReader) throws IOException {
		ArrayList<ArrayList<Integer>> adjList = new ArrayList<ArrayList<Integer>>();
		while(adjList.size() < numProcess) {
			adjList.add(new ArrayList<Integer>());
		}
		for (Integer i = 0; i < numProcess; i++) {
			String tempLine;
			tempLine = inReader.readLine();
			String[] nums = tempLine.split(" ");
			Integer curr = Integer.parseInt(nums[0]);
			curr--;
			for(Integer j = 1; j < nums.length; j++) {
				Integer x = Integer.parseInt(nums[j]);
				x--;
				adjList.get(curr).add(x);
				if(!isDirected) {
					adjList.get(x).add(curr);
				}
			}
		}
		return adjList;
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Usage: java ChandyANDModel <id> <Probe flag>");
			System.exit(-1);
		}

		myId = Integer.parseInt(args[0]);
		myId--;
		
		if(Integer.parseInt(args[1]) == 1) {
			probeFlag = true;
		}
		
		BufferedReader inReader = new BufferedReader(new FileReader("inp-params.txt"));
		String tempLine = inReader.readLine();
		String[] nums = tempLine.split(" ");
		numProcess = Integer.parseInt(nums[0]);

		for (Integer i = 0; i < numProcess; i++) {
			tempLine = inReader.readLine();
			nums = tempLine.split(" ");
			ipList.add(nums[2]);
		}

		while(adjList.size() < numProcess) {
			adjList.add(new ArrayList<Integer>());
			wfgList.add(new ArrayList<Integer>());
			routing.add(new ArrayList<Integer>());
		}

		adjList = getList(false, inReader);
		wfgList = getList(true, inReader);

		for(int i = 0; i < numProcess; i++) {	//Making adjacency lists unique
			HashSet<Integer> temp = new HashSet<>();
			temp.addAll(adjList.get(i));
			adjList.get(i).clear();
			adjList.get(i).addAll(temp);
			Collections.sort(adjList.get(i));

			temp = new HashSet<>();
			temp.addAll(wfgList.get(i));
			wfgList.get(i).clear();
			wfgList.get(i).addAll(temp);
			for(int j = 0; j < numProcess; j++) {
				routing.get(i).add(0);
			}
		}

		//Routing needed
		for(Integer i = 0; i < numProcess; i++) {
			if(!bfs(i)) {
				System.out.println("Topology not connected");
				System.exit(-1);
			}
		}
			
		for(Integer neigh: adjList.get(myId)) {	//Establising connections
			if(neigh < myId) {
				while(true) {
					try {
						Socket s = new Socket(ipList.get(neigh), getHash(neigh, myId));
						s.setSoTimeout(1000);
						socketMap.put(neigh, s);
						break;
					} catch (ConnectException e) {
						continue;
					}
				}
			} else {
				ServerSocket ss = new ServerSocket(getHash(neigh, myId));
				Socket s = ss.accept();
				s.setSoTimeout(1000);
				socketMap.put(neigh, s);
			}
		}

		if (wfgList.get(myId).size() > 0) {
			myType = processType.WAITING;
		}

		dowork(routing.get(myId));
	}
}
