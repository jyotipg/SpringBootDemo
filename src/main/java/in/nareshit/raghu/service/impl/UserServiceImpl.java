package in.nareshit.raghu.service.impl;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import in.nareshit.raghu.entity.User;
import in.nareshit.raghu.entity.UserRequest;
import in.nareshit.raghu.repository.UserRepository;
import in.nareshit.raghu.service.IUserService;

@Service
public class UserServiceImpl implements IUserService{
	@Autowired
	private UserRepository repository;


	@Transactional
	public Integer saveUser(User user) {
		return repository.save(user).getId();
	}

	@Transactional(readOnly = true)
	public User findByUsername(String username) {
		Optional<User> user=repository.findByUsername(username);
		if(user.isPresent()) 
			return user.get();
		return null;
	}

	@Override
	public boolean isUserExist(UserRequest userRequest) {
		User user = findByUsername(userRequest.getUsername());
		if(user.getPassword().equals(userRequest.getPassword()))
			return true;
		else 
			return false;
	}

}
