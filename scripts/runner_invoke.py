import sys, traceback
sys.path.insert(0, r'C:\Users\Dell\Documents\ryanfacul\sd\peerToPeer\scripts')
import run_real_tests as runner

def main():
    try:
        rc = runner.main()
        print('RUNNER_EXIT', rc)
    except SystemExit as e:
        print('SystemExit', e.code)
        raise
    except Exception:
        traceback.print_exc()
        raise

if __name__ == '__main__':
    main()
