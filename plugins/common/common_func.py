def get_sftp():
    print("sftp 작업 실행")
    
def regist2(name, sex, *args, **kwargs):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타옵션들: {args}')
    email = kwargs['email'] or None
    phone = kwargs['phone'] or None
    
    if email:
        print(email)
    if phone:
        print(phone)