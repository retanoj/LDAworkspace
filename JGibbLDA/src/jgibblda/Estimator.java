/*
 * Copyright (C) 2007 by
 * 
 * 	Xuan-Hieu Phan
 *	hieuxuan@ecei.tohoku.ac.jp or pxhieu@gmail.com
 * 	Graduate School of Information Sciences
 * 	Tohoku University
 * 
 *  Cam-Tu Nguyen
 *  ncamtu@gmail.com
 *  College of Technology
 *  Vietnam National University, Hanoi
 *
 * JGibbsLDA is a free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation; either version 2 of the License,
 * or (at your option) any later version.
 *
 * JGibbsLDA is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JGibbsLDA; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 */

package jgibblda;

import java.io.File;
import java.util.Vector;
import static java.lang.Math.log;
import static java.lang.Math.exp;

public class Estimator {
	
	// output model
	protected Model trnModel;
	LDACmdOption option;
	
	public boolean init(LDACmdOption option){
		this.option = option;
		trnModel = new Model();
		
		if (option.est){
			if (!trnModel.initNewModel(option))
				return false;
			trnModel.data.localDict.writeWordMap(option.dir + File.separator + option.wordMapFileName);
		}
		else if (option.estc){
			if (!trnModel.initEstimatedModel(option))
				return false;
		}
		
		return true;
	}
	
	public void estimate(){
		//System.out.println("Sampling " + trnModel.niters + " iteration!");
		
		int lastIter = trnModel.liter;
		for (trnModel.liter = lastIter + 1; trnModel.liter < trnModel.niters + lastIter; trnModel.liter++){
			//System.out.println("Iteration " + trnModel.liter + " ...");
			
			for (int m = 0; m < trnModel.M; m++){ //对于每一篇文档
				for (int n = 0; n < trnModel.data.docs[m].length; n++){ //对于每篇文档的每个位置
					int topic = sampling(m, n);
					trnModel.z[m].set(n, topic);
				}
			}
			
			if (option.savestep > 0){
				if (trnModel.liter % option.savestep == 0){
					computeTheta();
					computePhi();
					computeOmiga();
					//trnModel.saveModel("model-" + Conversion.ZeroPad(trnModel.liter, 5));
					System.out.println(String.format("llh:%f",computeLogLikelihood()));
				}
			}
		}		
		
		computeTheta();
		computePhi();
		computeOmiga();
		trnModel.liter--;
		trnModel.saveModel("model-final");
		System.out.println(String.format("for topicn=%d, llh=%f", trnModel.K, computeLogLikelihood()));
	}
	
	/**
	 * Do sampling
	 * @param m document number
	 * @param n word number
	 * @return topic id
	 */
	public int sampling(int m, int n){
		// remove z_i from the count variable
		int topic = trnModel.z[m].get(n);       //获得第m篇文档第n个位置的主题
		int w = trnModel.data.docs[m].words[n]; //获得第m篇文档第n个位置的词
		int x = trnModel.data.docs[m].area;		//获得第m篇文档所属的区域x
		
		trnModel.nw[w][topic] -= 1; //词w分到主题t的次数减1
		trnModel.nd[m][topic] -= 1; //文档m中分到主题t的词的个数减1
		trnModel.nx[x][topic] -= 1; //区域x中分到主题t的词的个数减1
		trnModel.nwsum[topic] -= 1; //分到t主题的词的个数减1
		trnModel.ndsum[m]     -= 1; //文档m中词的个数减1
		trnModel.nxsum[x]     -= 1;	//区域x中词的个数减1
		
		double Vbeta  = trnModel.V * trnModel.beta ;
		double Kalpha = trnModel.K * trnModel.alpha;
		double Kgamma = trnModel.K * trnModel.gamma;
		
		//do multinominal sampling via cumulative method
		for (int k = 0; k < trnModel.K; k++){
			trnModel.p[k] = (trnModel.nw[w][k] + trnModel.beta )/(trnModel.nwsum[k] + Vbeta )
							*(trnModel.nd[m][k] + trnModel.alpha)/(trnModel.ndsum[m] + Kalpha)
							*( 1.0 / ( 1.0 + exp( -( (double)trnModel.nx[x][k] / trnModel.nxsum[x] - 1.0 / trnModel.K ) ) ) + 0.5);
		
//		System.out.println(String.format("%f * %f * %f", (trnModel.nw[w][k] + trnModel.beta )/(trnModel.nwsum[k] + Vbeta ),
//														(trnModel.nd[m][k] + trnModel.alpha)/(trnModel.ndsum[m] + Kalpha),
//														( 1.0 / ( 1.0 + exp( -( (double)trnModel.nx[x][k] / trnModel.nxsum[x] - 1.0 / trnModel.K ) / trnModel.K ) ) + 0.5)));
		}
		
		// cumulate multinomial parameters
		for (int k = 1; k < trnModel.K; k++){
			trnModel.p[k] += trnModel.p[k - 1];
		}
		
		// scaled sample because of unnormalized p[]
		double u = Math.random() * trnModel.p[trnModel.K - 1];
		
		for (topic = 0; topic < trnModel.K; topic++){
			if (trnModel.p[topic] > u) //sample topic w.r.t distribution p
				break;
		}
		
		// add newly estimated z_i to count variables
		trnModel.nw[w][topic] += 1;
		trnModel.nd[m][topic] += 1;
		trnModel.nx[x][topic] += 1;
		trnModel.nwsum[topic] += 1;
		trnModel.ndsum[m]     += 1;
		trnModel.nxsum[x]     += 1;
		
 		return topic;
	}
	
	public void computeTheta(){
		for (int m = 0; m < trnModel.M; m++){
			for (int k = 0; k < trnModel.K; k++){
				trnModel.theta[m][k] = (trnModel.nd[m][k] + trnModel.alpha) / (trnModel.ndsum[m] + trnModel.K * trnModel.alpha);
			}
		}
	}
	
	public void computePhi(){
		for (int k = 0; k < trnModel.K; k++){
			for (int w = 0; w < trnModel.V; w++){
				trnModel.phi[k][w] = (trnModel.nw[w][k] + trnModel.beta) / (trnModel.nwsum[k] + trnModel.V * trnModel.beta);
			}
		}
	}
	
	public void computeOmiga(){
		for (int x = 0; x < trnModel.X; x++){
			for (int k = 0; k < trnModel.K; k++){
				trnModel.omiga[x][k] = (trnModel.nx[x][k] + trnModel.gamma) / (trnModel.nxsum[x] + trnModel.K * trnModel.gamma);
			}
		}
	}
	
	public double computeLogLikelihood(){
		double loglikelihood = 0;
		int w,x;
		for (int m = 0; m < trnModel.M; m++){
			for (int n = 0; n < trnModel.data.docs[m].length; n++){
				double p = 0;
				w = trnModel.data.docs[m].words[n]; //获得第m篇文档第n个位置的词
				x = trnModel.data.docs[m].area;		//获得第m篇文档所属的区域x
				for (int k = 0; k < trnModel.K; k++){
					p  +=  trnModel.theta[m][k] 
						 * trnModel.omiga[x][k] 
						 * trnModel.phi[k][w];
				}
				loglikelihood += log(p);
			}
		}
		
		return loglikelihood;
	}
}
