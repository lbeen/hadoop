package hadoop.rpc.bo;

/**
 * @author 李斌
 */
public class ServiceImpl implements ServiceInterface {
    @Override
    public String login(String username) {
        return username + "logined!";
    }
}
