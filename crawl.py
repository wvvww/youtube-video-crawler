if __name__ == "__main__":
    from crawler.controllers import Controller
    from crawler.arguments import parse_args

    controller = Controller(parse_args())
    controller.start()