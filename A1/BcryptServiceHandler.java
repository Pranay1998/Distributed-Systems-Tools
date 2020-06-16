import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
    {
	try {
		List<String> ret = new ArrayList<>();
		for (int i = 0; i < password.size(); i++) {
			ret.add(BCrypt.hashpw(password.get(i), Bcrypt.gensalt(logRounds)));
		}
	    return ret;
	} catch (Exception e) {
	    throw new IllegalArgument(e.getMessage());
	}
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
    {
	try {
		List<Boolean> ret = new ArrayList<>();
		for (int i = 0; i < password.size(); i++) {
			ret.add(BCrypt.checkpw(password.get(i), hash.get(i)));
		}
	    return ret;
	} catch (Exception e) {
	    throw new IllegalArgument(e.getMessage());
	}
    }
}
