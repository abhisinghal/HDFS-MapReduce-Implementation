package TaskTracker;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;


@SuppressWarnings("serial")
public class ITaskTrackerRemote extends UnicastRemoteObject implements ITaskTracker
{
	
	ITaskTrackerRemote() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public byte[] jobSubmit(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

}
