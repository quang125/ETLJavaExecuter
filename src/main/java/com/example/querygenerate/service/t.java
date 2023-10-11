package com.example.querygenerate.service;

import java.util.*;

/**
 * @author QuangNN
 */
public class t {
    static int[] d;
    static List<List<Integer>>g;
    public static void d(int i){
        for(int x:g.get(i)){
            if(d[x]==-1){
                d[x]=d[i];
                d(x);
            }
        }
    }

    public static void main(String[] args) {
        int[][] edge=new int[][]{{}};
        d=new int[edge.length];
        Arrays.fill(d,-1);
        g=new ArrayList<>();
        for(int[] x:edge){
            g.get(x[0]).add(x[1]);
        }
        int c=0;
        for(int i=0;i<edge.length;i++){
            if(d[i]==-1){
                d[i]=c;
                d(i);
                c+=1;
            }
        }
        int n=9;
        Map<Integer,List<Integer>>m=new HashMap<>();
        for(int i=0;i<n;i++){
           if(!m.containsKey(d[i])) m.put(d[i],new ArrayList<>());
           m.get(d[i]).add(i);
        }
        int[] cnt=new int[n];
        for(int[] x:edge){
            cnt[x[1]]+=1;
        }
        int[] res=new int[edge.length];
        Set<Integer>s=new HashSet<>();
        for(int x:m.keySet()){
            Queue<int[]>q=new ArrayDeque<>();
            for(int i:m.get(x)){
                if(cnt[i]==0){
                    s.add(i);
                }
            }
            int w=0;
            while(q.size()>0){
                int[] z=q.poll();
                w+=1;
                for(int u: g.get(z[0])){
                    if(s.contains(u)) continue;
                    cnt[u]-=1;
                    if(cnt[u]==0) q.add(new int[]{u,z[1]+1});
                }
            }
//            for(int u:m.get(x)){
//                if(!s.contains())
//            }
        }
    }
}
