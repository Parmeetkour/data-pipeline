from abc import ABC, abstractmethod
class BaseHandler(ABC):
    """
    BaseHandler is the base class, to be inherited by all the other handlers,
    depending on several sources like S3, MySQL, Vertica etc.
    It enforces every class that inherits BaseHandler to define a set of methods
    in order to keep the flow for source_to_landing and landing_to_parsed stages
    generic and scalable.
    Instructions to use:
    1. The class name should refer to the source, i.e. S3Handler, MYSQLHandler etc
    2. The class should inherit BaseHandler
    3. The class must implement the following methods:
        - init_handler
        - do_source_to_landing
        - do_landing_to_parsed
    """
    @abstractmethod
    def do_source_to_landing(self,  **kwargs):
        """
        This method will be responsible to execute the workflow of generating
        data from source layer to landing layer in appropriate format depending
        on the source it is using
        """
        pass

    @abstractmethod
    def do_landing_to_parsed(self, kwargs):
        """
        This method will be responsible to execute the workflow of copying data
        from landing layer to parsed layer as per the logic required for the source
        and the files
        """
        pass