package hadoop.rpc.bo;


public interface ServiceInterface {
    long versionID = 1L;


    String login(String username);

}
