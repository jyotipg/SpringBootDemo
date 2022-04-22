package in.nareshit.raghu.service;

import in.nareshit.raghu.entity.User;
import in.nareshit.raghu.entity.UserRequest;

public interface IUserService {

	public Integer saveUser(User user);
	public User findByUsername(String username);
	public boolean isUserExist(UserRequest userRequest);
}
