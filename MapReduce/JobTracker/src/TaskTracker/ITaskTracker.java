package TaskTracker;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ITaskTracker extends Remote
{
		/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	byte[] jobSubmit(byte[] inp) throws RemoteException;

	
}
